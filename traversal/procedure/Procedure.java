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
import grakn.core.common.producer.Producer;
import grakn.core.graph.GraphManager;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.planner.Planner;
import grakn.core.traversal.planner.PlannerEdge;
import grakn.core.traversal.planner.PlannerVertex;
import grakn.core.traversal.producer.GraphProducer;
import grakn.core.traversal.producer.VertexProducer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class Procedure {

    private final Map<Identifier, ProcedureVertex<?, ?>> vertices;
    private final ArrayList<ProcedureEdge<?, ?>> edges;
    private ProcedureVertex<?, ?> startVertex;

    private Procedure(int edgeSize) {
        vertices = new HashMap<>();
        edges = new ArrayList<>(edgeSize);
    }

    public static Procedure create(Planner planner) {
        Procedure procedure = new Procedure(planner.edges().size());
        Set<PlannerVertex<?>> registeredVertices = new HashSet<>();
        Set<PlannerEdge.Directional<?, ?>> registeredEdges = new HashSet<>();
        planner.vertices().forEach(vertex -> procedure.registerVertex(vertex, registeredVertices, registeredEdges));
        return procedure;
    }

    public Stream<ProcedureVertex<?, ?>> vertices() {
        return vertices.values().stream();
    }

    public ProcedureVertex<?, ?> vertex(Identifier identifier) {
        return vertices.get(identifier);
    }

    public ProcedureEdge<?, ?> edge(int pos) {
        return edges.get(pos - 1);
    }

    public int edgesCount() {
        return edges.size();
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
        edges.set(edge.order() - 1, edge);
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

    public Producer<VertexMap> execute(GraphManager graphMgr, Traversal.Parameters parameters, int parallelisation) {
        if (edgesCount() == 0) return new VertexProducer(graphMgr, this, parameters);
        else return new GraphProducer(graphMgr, this, parameters, parallelisation);
    }

    public ProcedureVertex<?, ?> startVertex() {
        if (startVertex == null) {
            startVertex = this.vertices().filter(ProcedureVertex::isStartingVertex)
                    .findAny().orElseThrow(() -> GraknException.of(ILLEGAL_STATE));
        }
        return startVertex;
    }
}
