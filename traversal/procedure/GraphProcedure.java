/*
 * Copyright (C) 2020 Grakn Labs
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
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.producer.Producer;
import grakn.core.graph.GraphManager;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.planner.GraphPlanner;
import grakn.core.traversal.planner.PlannerEdge;
import grakn.core.traversal.planner.PlannerVertex;
import grakn.core.traversal.producer.GraphIterator;
import grakn.core.traversal.producer.GraphProducer;
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
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

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
        edges[edge.order() - 1] = edge;
        from.out(edge);
        to.in(edge);
    }

    private ProcedureVertex<?, ?> vertex(PlannerVertex<?> plannerVertex) {
        if (plannerVertex.isThing()) return thingVertex(plannerVertex.asThing());
        else return typeVertex(plannerVertex.asType());
    }

    private ProcedureVertex.Thing thingVertex(PlannerVertex.Thing plannerVertex) {
        return vertices.computeIfAbsent(
                plannerVertex.id(),
                id -> new ProcedureVertex.Thing(id, plannerVertex.isStartingVertex())
        ).asThing();
    }

    private ProcedureVertex.Type typeVertex(PlannerVertex.Type plannerVertex) {
        return vertices.computeIfAbsent(
                plannerVertex.id(),
                id -> new ProcedureVertex.Type(id, plannerVertex.isStartingVertex())
        ).asType();
    }

    @Override
    public Producer<VertexMap> producer(GraphManager graphMgr, Traversal.Parameters params, int parallelisation) {
        LOG.debug(toString()); // TODO: remove this
        return new GraphProducer(graphMgr, this, params, parallelisation);
    }

    @Override
    public ResourceIterator<VertexMap> iterator(GraphManager graphMgr, Traversal.Parameters params) {
        LOG.debug(toString()); // TODO: remove this
        return startVertex().iterator(graphMgr, params).flatMap(
                sv -> new GraphIterator(graphMgr, sv, this, params)
        ).distinct();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        List<ProcedureEdge<?, ?>> procedureEdges = Arrays.asList(edges);
        procedureEdges.sort(Comparator.comparing(ProcedureEdge::order));
        List<ProcedureVertex<?, ?>> procedureVertices = new ArrayList<>(vertices.values());
        procedureVertices.sort(Comparator.comparing(v -> v.id().toString()));

        str.append("\n");
        str.append("Edges:\n");
        for (ProcedureEdge<?, ?> e : procedureEdges) {
            str.append(e).append("\n");
        }
        str.append("\n");
        str.append("Vertices:\n");
        for (ProcedureVertex<?, ?> v : procedureVertices) {
            str.append(v).append("\n");
        }
        return str.toString();
    }
}
