/*
 * Copyright (C) 2021 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.traversal.procedure;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.common.parameters.Label;
import grakn.common.collection.ConcurrentSet;
import grakn.core.concurrent.producer.FunctionalProducer;
import grakn.core.graph.GraphManager;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.iterator.GraphIterator;
import grakn.core.traversal.planner.GraphPlanner;
import grakn.core.traversal.planner.PlannerEdge;
import grakn.core.traversal.planner.PlannerVertex;
import grakn.core.traversal.predicate.Predicate;
import graql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.concurrent.producer.Producers.async;

public class GraphProcedure implements Procedure {

    private static final Logger LOG = LoggerFactory.getLogger(GraphProcedure.class);

    private final Map<Identifier, ProcedureVertex<?, ?>> vertices;
    private final ProcedureEdge<?, ?>[] edges;
    private ProcedureVertex<?, ?> startVertex;

    private GraphProcedure(int edgeSize) {
        vertices = new HashMap<>();
        edges = new ProcedureEdge<?, ?>[edgeSize];
    }

    public static GraphProcedure create(GraphPlanner planner) {
        GraphProcedure procedure = new GraphProcedure(planner.edges().size());
        Set<PlannerVertex<?>> registeredVertices = new HashSet<>();
        Set<PlannerEdge.Directional<?, ?>> registeredEdges = new HashSet<>();
        planner.vertices().forEach(vertex -> procedure.registerVertex(vertex, registeredVertices, registeredEdges));
        return procedure;
    }

    public static GraphProcedure.Builder builder(int size) {
        GraphProcedure procedure = new GraphProcedure(size);
        return procedure.new Builder();
    }

    public Stream<ProcedureVertex<?, ?>> vertices() {
        return vertices.values().stream();
    }

    public ProcedureVertex<?, ?> startVertex() {
        if (startVertex == null) {
            startVertex = this.vertices().filter(ProcedureVertex::isStartingVertex)
                    .findAny().orElseThrow(() -> GraknException.of(ILLEGAL_STATE));
        }
        return startVertex;
    }

    public ProcedureVertex<?, ?> vertex(Identifier identifier) {
        return vertices.get(identifier);
    }

    public ProcedureEdge<?, ?> edge(int pos) {
        return edges[pos - 1];
    }

    public int edgesCount() {
        return edges.length;
    }

    private void registerVertex(PlannerVertex<?> plannerVertex, Set<PlannerVertex<?>> registeredVertices,
                                Set<PlannerEdge.Directional<?, ?>> registeredEdges) {
        if (registeredVertices.contains(plannerVertex)) return;
        registeredVertices.add(plannerVertex);
        List<PlannerVertex<?>> adjacents = new ArrayList<>();
        ProcedureVertex<?, ?> vertex = vertex(plannerVertex);
        if (vertex.isThing()) vertex.asThing().props(plannerVertex.asThing().props());
        else vertex.asType().props(plannerVertex.asType().props());
        plannerVertex.outs().forEach(plannerEdge -> {
            if (!registeredEdges.contains(plannerEdge) && plannerEdge.isSelected()) {
                registeredEdges.add(plannerEdge);
                adjacents.add(plannerEdge.to());
                registerEdge(plannerEdge);
            }
        });
        plannerVertex.ins().forEach(plannerEdge -> {
            if (!registeredEdges.contains(plannerEdge) && plannerEdge.isSelected()) {
                registeredEdges.add(plannerEdge);
                adjacents.add(plannerEdge.from());
                registerEdge(plannerEdge);
            }
        });
        adjacents.forEach(v -> registerVertex(v, registeredVertices, registeredEdges));
    }

    private void registerEdge(PlannerEdge.Directional<?, ?> plannerEdge) {
        ProcedureVertex<?, ?> from = vertex(plannerEdge.from());
        ProcedureVertex<?, ?> to = vertex(plannerEdge.to());
        ProcedureEdge<?, ?> edge = ProcedureEdge.of(from, to, plannerEdge);
        registerEdge(edge);
    }

    public void registerEdge(ProcedureEdge<?, ?> edge) {
        edges[edge.order() - 1] = edge;
        edge.from().out(edge);
        edge.to().in(edge);
    }

    private ProcedureVertex<?, ?> vertex(PlannerVertex<?> plannerVertex) {
        if (plannerVertex.isThing()) return thingVertex(plannerVertex.asThing());
        else return typeVertex(plannerVertex.asType());
    }

    private ProcedureVertex.Thing thingVertex(PlannerVertex.Thing plannerVertex) {
        return thingVertex(plannerVertex.id(), plannerVertex.isStartingVertex());
    }

    private ProcedureVertex.Type typeVertex(PlannerVertex.Type plannerVertex) {
        return typeVertex(plannerVertex.id(), plannerVertex.isStartingVertex());
    }

    private ProcedureVertex.Thing thingVertex(Identifier identifier, boolean isStart) {
        return vertices.computeIfAbsent(
                identifier, id -> new ProcedureVertex.Thing(id, isStart)
        ).asThing();
    }

    private ProcedureVertex.Type typeVertex(Identifier identifier, boolean isStart) {
        return vertices.computeIfAbsent(
                identifier, id -> new ProcedureVertex.Type(id, isStart)
        ).asType();
    }

    private void assertWithinFilterBounds(Set<Identifier.Variable.Retrievable> filter) {
        assert iterate(vertices.keySet()).anyMatch(filter::contains);
    }

    @Override
    public FunctionalProducer<VertexMap> producer(GraphManager graphMgr, Traversal.Parameters params,
                                                  Set<Identifier.Variable.Retrievable> filter, int parallelisation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(params.toString());
            LOG.debug(this.toString());
        }
        assertWithinFilterBounds(filter);
        ConcurrentSet<VertexMap> produced = new ConcurrentSet<>();
        FunctionalIterator<FunctionalIterator<VertexMap>> iterators = startVertex().iterator(graphMgr, params)
                .map(v -> new GraphIterator(graphMgr, v, this, params, filter).distinct(produced));
        return async(iterators, parallelisation);
    }

    @Override
    public FunctionalIterator<VertexMap> iterator(GraphManager graphMgr, Traversal.Parameters params,
                                                  Set<Identifier.Variable.Retrievable> filter) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(params.toString());
            LOG.debug(this.toString());
        }
        assertWithinFilterBounds(filter);
        return startVertex().iterator(graphMgr, params).flatMap(
                sv -> new GraphIterator(graphMgr, sv, this, params, filter)
        ).distinct();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("Graph Procedure: {");
        List<ProcedureEdge<?, ?>> procedureEdges = Arrays.asList(edges);
        procedureEdges.sort(Comparator.comparing(ProcedureEdge::order));
        List<ProcedureVertex<?, ?>> procedureVertices = new ArrayList<>(vertices.values());
        procedureVertices.sort(Comparator.comparing(v -> v.id().toString()));

        str.append("\n\tvertices:");
        for (ProcedureVertex<?, ?> v : procedureVertices) {
            str.append("\n\t\t").append(v);
        }
        str.append("\n\tedges:");
        for (ProcedureEdge<?, ?> e : procedureEdges) {
            str.append("\n\t\t").append(e);
        }
        str.append("\n}");
        return str.toString();
    }

    public class Builder { // TODO: to be completed

        public GraphProcedure build() {
            for (ProcedureEdge<?, ?> edge : edges) assert edge != null;
            vertices.values().forEach(v -> {
                if (v.isType()) v.asType().props(v.asType().props());
            });
            return GraphProcedure.this;
        }

        public ProcedureVertex.Type labelledType(String label) {
            return labelledType(label, false);
        }

        public ProcedureVertex.Type labelledType(String label, boolean isStart) {
            return typeVertex(Identifier.Variable.of(Reference.label(label)), isStart);
        }

        public ProcedureVertex.Type namedType(String name) {
            return namedType(name, false);
        }

        public ProcedureVertex.Type namedType(String name, boolean isStart) {
            return typeVertex(Identifier.Variable.of(Reference.name(name)), isStart);
        }

        public ProcedureVertex.Thing namedThing(String name) {
            return namedThing(name, false);
        }

        public ProcedureVertex.Thing namedThing(String name, boolean isStart) {
            return thingVertex(Identifier.Variable.of(Reference.name(name)), isStart);
        }

        public ProcedureVertex.Thing anonymousThing(int id) {
            return thingVertex(Identifier.Variable.anon(id), false);
        }

        public ProcedureVertex.Thing scopedThing(ProcedureVertex.Thing relation, ProcedureVertex.Type roleType, ProcedureVertex.Thing player, int repetition) {
            return thingVertex(Identifier.Scoped.of(relation.id().asVariable(), roleType.id().asVariable(), player.id().asVariable(), repetition), false);
        }

        public ProcedureVertex.Type setLabel(ProcedureVertex.Type type, String label) {
            type.props().labels(Label.of(label));
            return type;
        }

        public ProcedureVertex.Type setLabel(ProcedureVertex.Type type, String label, String scope) {
            type.props().labels(Label.of(label, scope));
            return type;
        }

        public ProcedureVertex.Thing setTypes(ProcedureVertex.Thing thing, Set<String> types) {
            thing.props().types(types.stream().map(x -> Label.of(x)).collect(Collectors.toSet()));
            return thing;
        }

        public ProcedureVertex.Thing setPredicate(ProcedureVertex.Thing thing, Predicate.Value.String predicate) {
            thing.props().predicate(predicate);
            return thing;
        }

        public ProcedureEdge.Native.Isa.Forward forwardIsa(
                int order, ProcedureVertex.Thing thing, ProcedureVertex.Type type, boolean isTransitive) {
            ProcedureEdge.Native.Isa.Forward edge =
                    new ProcedureEdge.Native.Isa.Forward(thing, type, order, isTransitive);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Isa.Backward backwardIsa(
                int order, ProcedureVertex.Type type, ProcedureVertex.Thing thing, boolean isTransitive) {
            ProcedureEdge.Native.Isa.Backward edge =
                    new ProcedureEdge.Native.Isa.Backward(type, thing, order, isTransitive);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Type.Owns.Forward forwardOwns(
                int order, ProcedureVertex.Type owner, ProcedureVertex.Type att, boolean isKey) {
            ProcedureEdge.Native.Type.Owns.Forward edge =
                    new ProcedureEdge.Native.Type.Owns.Forward(owner, att, order, isKey);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Type.Owns.Backward backwardOwns(
                int order, ProcedureVertex.Type att, ProcedureVertex.Type owner, boolean isKey) {
            ProcedureEdge.Native.Type.Owns.Backward edge =
                    new ProcedureEdge.Native.Type.Owns.Backward(att, owner, order, isKey);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Thing.Has.Forward forwardHas(
                int order, ProcedureVertex.Thing owner, ProcedureVertex.Thing attribute) {
            ProcedureEdge.Native.Thing.Has.Forward edge =
                    new ProcedureEdge.Native.Thing.Has.Forward(owner, attribute, order);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Thing.Has.Backward backwardHas(
                int order, ProcedureVertex.Thing attribute, ProcedureVertex.Thing owner) {
            ProcedureEdge.Native.Thing.Has.Backward edge =
                    new ProcedureEdge.Native.Thing.Has.Backward(attribute, owner, order);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Thing.Relating.Forward forwardRelating(
                int order, ProcedureVertex.Thing relation, ProcedureVertex.Thing role) {
            ProcedureEdge.Native.Thing.Relating.Forward edge =
                    new ProcedureEdge.Native.Thing.Relating.Forward(relation, role, order);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Thing.Relating.Backward backwardRelating(
                int order, ProcedureVertex.Thing role, ProcedureVertex.Thing relation) {
            ProcedureEdge.Native.Thing.Relating.Backward edge =
                    new ProcedureEdge.Native.Thing.Relating.Backward(role, relation, order);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Thing.Playing.Forward forwardPlaying(
                int order, ProcedureVertex.Thing player, ProcedureVertex.Thing role) {
            ProcedureEdge.Native.Thing.Playing.Forward edge =
                    new ProcedureEdge.Native.Thing.Playing.Forward(player, role, order);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Thing.Playing.Backward backwardPlaying(
                int order, ProcedureVertex.Thing role, ProcedureVertex.Thing player) {
            ProcedureEdge.Native.Thing.Playing.Backward edge =
                    new ProcedureEdge.Native.Thing.Playing.Backward(role, player, order);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Thing.RolePlayer.Forward forwardRolePlayer(
                int order, ProcedureVertex.Thing relation, ProcedureVertex.Thing player, Set<Label> roleTypes) {
            ProcedureEdge.Native.Thing.RolePlayer.Forward edge =
                    new ProcedureEdge.Native.Thing.RolePlayer.Forward(relation, player, order, roleTypes);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Thing.RolePlayer.Backward backwardRolePlayer(
                int order, ProcedureVertex.Thing player, ProcedureVertex.Thing relation, Set<Label> roleTypes) {
            ProcedureEdge.Native.Thing.RolePlayer.Backward edge =
                    new ProcedureEdge.Native.Thing.RolePlayer.Backward(player, relation, order, roleTypes);
            registerEdge(edge);
            return edge;
        }
    }
}
