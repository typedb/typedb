/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.traversal.procedure;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concurrent.producer.FunctionalProducer;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.graph.TraversalVertex;
import com.vaticle.typedb.core.traversal.planner.GraphPlanner;
import com.vaticle.typedb.core.traversal.planner.PlannerEdge;
import com.vaticle.typedb.core.traversal.planner.PlannerVertex;
import com.vaticle.typedb.core.traversal.predicate.Predicate;
import com.vaticle.typedb.core.traversal.scanner.GraphIterator;
import com.vaticle.typedb.core.traversal.structure.Structure;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;
import com.vaticle.typeql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.concurrent.producer.Producers.async;
import static java.util.Comparator.comparing;

public class GraphProcedure implements PermutationProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(GraphProcedure.class);

    private final ProcedureVertex<?, ?>[] vertices;
    private Set<ProcedureVertex<?, ?>> startVertices;
    private Set<ProcedureVertex<?, ?>> endVertices;

    private GraphProcedure(ProcedureVertex<?, ?>[] vertices) {
        this.vertices = vertices;
    }

    public static GraphProcedure create(GraphPlanner planner) {
        Builder builder = new Builder();
        Set<PlannerVertex<?>> registeredVertices = new HashSet<>();
        Set<PlannerEdge.Directional<?, ?>> registeredEdges = new HashSet<>();
        planner.vertices().forEach(vertex -> builder.registerVertex(vertex, registeredVertices, registeredEdges));
        return builder.build();
    }

    public static GraphProcedure create(Structure structure, Map<Identifier, Integer> orders) {
        Builder builder = new Builder();
        structure.vertices().forEach(vertex -> builder.vertex(vertex).setOrder(orders.get(vertex.id())));
        structure.vertices().forEach(builder::registerEdges);
        return builder.build();
    }

    public ProcedureVertex<?, ?>[] vertices() {
        return vertices;
    }

    public ProcedureVertex<?, ?> initialVertex() {
        return vertices[0];
    }

    public Set<ProcedureVertex<?, ?>> startVertices() {
        if (startVertices == null) {
            startVertices = iterate(vertices()).filter(ProcedureVertex::isStartingVertex).toSet();
        }
        return startVertices;
    }

    public Set<ProcedureVertex<?, ?>> endVertices() {
        if (endVertices == null) {
            endVertices = iterate(vertices()).filter(v -> v.outs().isEmpty()).toSet();
        }
        return endVertices;
    }

    public ProcedureVertex<?, ?> vertex(int pos) {
        assert 0 <= pos && pos < vertices.length;
        return vertices[pos];
    }

    public int vertexCount() {
        return vertices.length;
    }

    private void assertWithinFilterBounds(Set<Identifier.Variable.Retrievable> filter) {
        assert iterate(vertices).anyMatch(vertex ->
                vertex.id().isRetrievable() && filter.contains(vertex.id().asVariable().asRetrievable())
        );
    }

    @Override
    public FunctionalProducer<VertexMap> producer(GraphManager graphMgr, Traversal.Parameters params,
                                                  Set<Identifier.Variable.Retrievable> filter, int parallelisation) {
        if (LOG.isTraceEnabled()) {
            LOG.trace(params.toString());
            LOG.trace(this.toString());
        }
        assertWithinFilterBounds(filter);
        if (initialVertex().id().isRetrievable() && filter.contains(initialVertex().id().asVariable().asRetrievable())) {
            return async(initialVertex().iterator(graphMgr, params).map(v ->
                    new GraphIterator(graphMgr, v, this, params, filter).distinct()
            ), parallelisation);
        } else {
            // TODO we can reduce the size of the distinct() set if the traversal engine doesn't overgenerate as much
            return async(initialVertex().iterator(graphMgr, params).map(v ->
                    new GraphIterator(graphMgr, v, this, params, filter)
            ), parallelisation).distinct();
        }
    }

    @Override
    public FunctionalIterator<VertexMap> iterator(GraphManager graphMgr, Traversal.Parameters params,
                                                  Set<Identifier.Variable.Retrievable> filter) {
        if (LOG.isTraceEnabled()) {
            LOG.trace(params.toString());
            LOG.trace(this.toString());
        }
        assertWithinFilterBounds(filter);
        if (initialVertex().id().isRetrievable() && filter.contains(initialVertex().id().asVariable().asRetrievable())) {
            return initialVertex().iterator(graphMgr, params).flatMap(
                    // TODO we can reduce the size of the distinct() set if the traversal engine doesn't overgenerate as much
                    sv -> new GraphIterator(graphMgr, sv, this, params, filter).distinct()
            );
        } else {
            // TODO we can reduce the size of the distinct() set if the traversal engine doesn't overgenerate as much
            return initialVertex().iterator(graphMgr, params).flatMap(
                    sv -> new GraphIterator(graphMgr, sv, this, params, filter)
            ).distinct();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphProcedure that = (GraphProcedure) o;
        return Arrays.equals(vertices, that.vertices);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(vertices);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("Graph Procedure: {");
        for (int i = 0; i < vertexCount(); i++) {
            ProcedureVertex<?, ?> vertex = vertices[i];
            str.append("\n\t").append(vertex);
            for (ProcedureEdge<?, ?> edge : vertex.ins()) {
                str.append("\n\t\t\t").append(edge);
            }
        }
        str.append("\n}");
        return str.toString();
    }

    public static class Builder {

        private final Map<Identifier, ProcedureVertex<?, ?>> vertices;

        public Builder() {
            this.vertices = new HashMap<>();
        }

        public GraphProcedure build() {
            return new GraphProcedure(vertices.values().stream().sorted(comparing(ProcedureVertex::order))
                    .toArray(ProcedureVertex[]::new));
        }

        private void registerVertex(PlannerVertex<?> plannerVertex, Set<PlannerVertex<?>> registeredVertices,
                                    Set<PlannerEdge.Directional<?, ?>> registeredEdges) {
            if (registeredVertices.contains(plannerVertex)) return;
            registeredVertices.add(plannerVertex);
            List<PlannerVertex<?>> adjacents = new ArrayList<>();

            ProcedureVertex<?, ?> vertex = vertex(plannerVertex);
            vertex.setOrder(plannerVertex.getOrder());

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
            plannerVertex.loops().forEach(plannerEdge -> {
                if (!registeredEdges.contains(plannerEdge) && plannerEdge.direction().isForward()) {
                    registeredEdges.add(plannerEdge);
                    registerEdge(plannerEdge);
                }
            });
            adjacents.forEach(v -> registerVertex(v, registeredVertices, registeredEdges));
        }

        private void registerEdges(StructureVertex<?> structureVertex) {
            assert vertices.containsKey(structureVertex.id());
            structureVertex.outs().forEach(e -> registerEdge(e, true));
            structureVertex.ins().forEach(e -> registerEdge(e, false));
            structureVertex.loops().forEach(e -> registerEdge(e, true));
        }

        private void registerEdge(StructureEdge<?, ?> structureEdge, boolean isForward) {
            ProcedureVertex<?, ?> from = vertex(structureEdge.from());
            ProcedureVertex<?, ?> to = vertex(structureEdge.to());
            if (!isForward) {
                ProcedureVertex<?, ?> tmp = to;
                to = from;
                from = tmp;
            }
            if (from.order() > to.order()) return;
            ProcedureEdge<?, ?> edge = ProcedureEdge.of(from, to, structureEdge, isForward);
            registerEdge(edge);
        }

        private void registerEdge(PlannerEdge.Directional<?, ?> plannerEdge) {
            ProcedureVertex<?, ?> from = vertex(plannerEdge.from());
            ProcedureVertex<?, ?> to = vertex(plannerEdge.to());
            ProcedureEdge<?, ?> edge = ProcedureEdge.of(from, to, plannerEdge);
            registerEdge(edge);
        }

        public void registerEdge(ProcedureEdge<?, ?> edge) {
            if (edge.from().equals(edge.to())) {
                edge.from().loop(edge);
            } else {
                edge.from().out(edge);
                edge.to().in(edge);
            }
        }

        private ProcedureVertex<?, ?> vertex(TraversalVertex<?, ?> traversalVertex) {
            if (traversalVertex.isThing()) {
                ProcedureVertex.Thing vertex = thingVertex(traversalVertex.id());
                vertex.asThing().props(traversalVertex.props().asThing());
                return vertex;
            } else {
                ProcedureVertex.Type vertex = typeVertex(traversalVertex.id());
                vertex.asType().props(traversalVertex.props().asType());
                return vertex;
            }
        }

        private ProcedureVertex.Thing thingVertex(PlannerVertex.Thing plannerVertex) {
            return thingVertex(plannerVertex.id());
        }

        private ProcedureVertex.Type typeVertex(PlannerVertex.Type plannerVertex) {
            return typeVertex(plannerVertex.id());
        }

        private ProcedureVertex.Thing thingVertex(Identifier identifier) {
            return vertices.computeIfAbsent(identifier, ProcedureVertex.Thing::new).asThing();
        }

        private ProcedureVertex.Type typeVertex(Identifier identifier) {
            return vertices.computeIfAbsent(identifier, ProcedureVertex.Type::new).asType();
        }

        // ---- manual builder methods ----

        public ProcedureVertex.Type labelledType(int order, String label) {
            ProcedureVertex.Type vertex = typeVertex(Identifier.Variable.of(Reference.label(label)));
            vertex.setOrder(order);
            return vertex;
        }

        public ProcedureVertex.Type namedType(int order, String name) {
            ProcedureVertex.Type vertex = typeVertex(Identifier.Variable.of(Reference.name(name)));
            vertex.setOrder(order);
            return vertex;
        }

        public ProcedureVertex.Thing namedThing(int order, String name) {
            ProcedureVertex.Thing vertex = thingVertex(Identifier.Variable.of(Reference.name(name)));
            vertex.setOrder(order);
            return vertex;
        }

        public ProcedureVertex.Thing anonymousThing(int order, int id) {
            ProcedureVertex.Thing vertex = thingVertex(Identifier.Variable.anon(id));
            vertex.setOrder(order);
            return vertex;
        }

        public ProcedureVertex.Thing scopedThing(int order, ProcedureVertex.Thing relation, @Nullable ProcedureVertex.Type roleType, ProcedureVertex.Thing player, int repetition) {
            ProcedureVertex.Thing vertex = thingVertex(Identifier.Scoped.of(relation.id().asVariable(), roleType != null ? roleType.id().asVariable() : null, player.id().asVariable(), repetition));
            vertex.setOrder(order);
            return vertex;
        }

        public ProcedureVertex.Type setLabel(ProcedureVertex.Type type, Label label) {
            type.props().labels(label);
            return type;
        }

        public ProcedureVertex.Type setLabels(ProcedureVertex.Type type, Set<Label> labels) {
            type.props().labels(labels);
            return type;
        }

        public ProcedureVertex.Thing setPredicate(ProcedureVertex.Thing thing, Predicate.Value.String predicate) {
            thing.props().predicate(predicate);
            return thing;
        }

        public ProcedureEdge.Native.Type.Sub.Forward forwardSub(
                ProcedureVertex.Type child, ProcedureVertex.Type parent, boolean isTransitive) {
            ProcedureEdge.Native.Type.Sub.Forward edge =
                    new ProcedureEdge.Native.Type.Sub.Forward(child, parent, isTransitive);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Type.Sub.Backward backwardSub(
                ProcedureVertex.Type parent, ProcedureVertex.Type child, boolean isTransitive) {
            ProcedureEdge.Native.Type.Sub.Backward edge =
                    new ProcedureEdge.Native.Type.Sub.Backward(parent, child, isTransitive);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Type.Plays.Forward forwardPlays(
                ProcedureVertex.Type player, ProcedureVertex.Type roleType) {
            ProcedureEdge.Native.Type.Plays.Forward edge =
                    new ProcedureEdge.Native.Type.Plays.Forward(player, roleType);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Type.Plays.Backward backwardPlays(
                ProcedureVertex.Type roleType, ProcedureVertex.Type player) {
            ProcedureEdge.Native.Type.Plays.Backward edge =
                    new ProcedureEdge.Native.Type.Plays.Backward(roleType, player);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Type.Owns.Forward forwardOwns(
                ProcedureVertex.Type owner, ProcedureVertex.Type att, boolean isKey) {
            ProcedureEdge.Native.Type.Owns.Forward edge =
                    new ProcedureEdge.Native.Type.Owns.Forward(owner, att, isKey);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Type.Owns.Backward backwardOwns(
                ProcedureVertex.Type att, ProcedureVertex.Type owner, boolean isKey) {
            ProcedureEdge.Native.Type.Owns.Backward edge =
                    new ProcedureEdge.Native.Type.Owns.Backward(att, owner, isKey);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Equal forwardEqual(ProcedureVertex.Type from, ProcedureVertex.Type to) {
            ProcedureEdge.Equal edge = new ProcedureEdge.Equal(from, to, Encoding.Direction.Edge.FORWARD);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Equal backwardEqual(ProcedureVertex.Type from, ProcedureVertex.Type to) {
            ProcedureEdge.Equal edge = new ProcedureEdge.Equal(from, to, Encoding.Direction.Edge.BACKWARD);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Predicate forwardPredicate(ProcedureVertex.Thing from, ProcedureVertex.Thing to, Predicate.Variable predicate) {
            ProcedureEdge.Predicate edge = new ProcedureEdge.Predicate(from, to, Encoding.Direction.Edge.FORWARD, predicate);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Predicate backwardPredicate(ProcedureVertex.Thing from, ProcedureVertex.Thing to, Predicate.Variable predicate) {
            ProcedureEdge.Predicate edge = new ProcedureEdge.Predicate(from, to, Encoding.Direction.Edge.BACKWARD, predicate);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Isa.Forward forwardIsa(
                ProcedureVertex.Thing thing, ProcedureVertex.Type type, boolean isTransitive) {
            ProcedureEdge.Native.Isa.Forward edge =
                    new ProcedureEdge.Native.Isa.Forward(thing, type, isTransitive);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Isa.Backward backwardIsa(
                ProcedureVertex.Type type, ProcedureVertex.Thing thing, boolean isTransitive) {
            ProcedureEdge.Native.Isa.Backward edge =
                    new ProcedureEdge.Native.Isa.Backward(type, thing, isTransitive);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Type.Relates.Forward forwardRelates(
                ProcedureVertex.Type relationType, ProcedureVertex.Type roleType) {
            ProcedureEdge.Native.Type.Relates.Forward edge =
                    new ProcedureEdge.Native.Type.Relates.Forward(relationType, roleType);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Type.Relates.Backward backwardRelates(
                ProcedureVertex.Type roleType, ProcedureVertex.Type relationType) {
            ProcedureEdge.Native.Type.Relates.Backward edge =
                    new ProcedureEdge.Native.Type.Relates.Backward(roleType, relationType);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Thing.Has.Forward forwardHas(
                ProcedureVertex.Thing owner, ProcedureVertex.Thing attribute) {
            ProcedureEdge.Native.Thing.Has.Forward edge =
                    new ProcedureEdge.Native.Thing.Has.Forward(owner, attribute);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Thing.Has.Backward backwardHas(
                ProcedureVertex.Thing attribute, ProcedureVertex.Thing owner) {
            ProcedureEdge.Native.Thing.Has.Backward edge =
                    new ProcedureEdge.Native.Thing.Has.Backward(attribute, owner);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Thing.Relating.Forward forwardRelating(
                ProcedureVertex.Thing relation, ProcedureVertex.Thing role) {
            ProcedureEdge.Native.Thing.Relating.Forward edge =
                    new ProcedureEdge.Native.Thing.Relating.Forward(relation, role);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Thing.Relating.Backward backwardRelating(
                ProcedureVertex.Thing role, ProcedureVertex.Thing relation) {
            ProcedureEdge.Native.Thing.Relating.Backward edge =
                    new ProcedureEdge.Native.Thing.Relating.Backward(role, relation);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Thing.Playing.Forward forwardPlaying(
                ProcedureVertex.Thing player, ProcedureVertex.Thing role) {
            ProcedureEdge.Native.Thing.Playing.Forward edge =
                    new ProcedureEdge.Native.Thing.Playing.Forward(player, role);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Thing.Playing.Backward backwardPlaying(
                ProcedureVertex.Thing role, ProcedureVertex.Thing player) {
            ProcedureEdge.Native.Thing.Playing.Backward edge =
                    new ProcedureEdge.Native.Thing.Playing.Backward(role, player);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Thing.RolePlayer.Forward forwardRolePlayer(
                ProcedureVertex.Thing relation, ProcedureVertex.Thing player, Set<Label> roleTypes) {
            ProcedureEdge.Native.Thing.RolePlayer.Forward edge =
                    new ProcedureEdge.Native.Thing.RolePlayer.Forward(relation, player, roleTypes);
            registerEdge(edge);
            return edge;
        }

        public ProcedureEdge.Native.Thing.RolePlayer.Backward backwardRolePlayer(
                ProcedureVertex.Thing player, ProcedureVertex.Thing relation, Set<Label> roleTypes) {
            ProcedureEdge.Native.Thing.RolePlayer.Backward edge =
                    new ProcedureEdge.Native.Thing.RolePlayer.Backward(player, relation, roleTypes);
            registerEdge(edge);
            return edge;
        }
    }
}
