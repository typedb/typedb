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

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.graph.GraphManager;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.Identifier;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.planner.Planner;
import grakn.core.traversal.planner.PlannerEdge;
import grakn.core.traversal.planner.PlannerVertex;
import graql.lang.pattern.variable.Reference;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grakn.core.common.iterator.Iterators.iterate;

public class Procedure {

    private final Map<Identifier, ProcedureVertex<?>> vertices;
    private final Set<ProcedureEdge> edges;

    private Procedure() {
        vertices = new HashMap<>();
        edges = new HashSet<>();
    }

    public static Procedure create(Planner planner) {
        Procedure procedure = new Procedure();
        Set<PlannerVertex<?>> registeredVertices = new HashSet<>();
        Set<PlannerEdge.Directional<?, ?>> registeredEdges = new HashSet<>();
        planner.vertices().forEach(vertex -> procedure.registerVertex(vertex, registeredVertices, registeredEdges));
        return procedure;
    }

    private void registerVertex(PlannerVertex<?> plannerVertex, Set<PlannerVertex<?>> registeredVertices,
                                Set<PlannerEdge.Directional<?, ?>> registeredEdges) {
        if (registeredVertices.contains(plannerVertex)) return;
        registeredVertices.add(plannerVertex);
        List<PlannerVertex<?>> adjacents = new ArrayList<>();
        ProcedureVertex<?> vertex = vertex(plannerVertex);
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
        ProcedureVertex<?> from = vertex(plannerEdge.from());
        ProcedureVertex<?> to = vertex(plannerEdge.to());
        ProcedureEdge edge = ProcedureEdge.of(from, to, plannerEdge);
        edges.add(edge);
        from.out(edge);
        to.in(edge);
    }

    private ProcedureVertex<?> vertex(PlannerVertex<?> plannerVertex) {
        if (plannerVertex.isThing()) return thingVertex(plannerVertex.asThing());
        else return typeVertex(plannerVertex.asType());
    }

    private ProcedureVertex.Thing thingVertex(PlannerVertex.Thing plannerVertex) {
        return vertices.computeIfAbsent(
                plannerVertex.identifier(), i -> new ProcedureVertex.Thing(i, this, plannerVertex.isStartingVertex())
        ).asThing();
    }

    private ProcedureVertex.Type typeVertex(PlannerVertex.Type plannerVertex) {
        return vertices.computeIfAbsent(
                plannerVertex.identifier(), i -> new ProcedureVertex.Type(i, this, plannerVertex.isStartingVertex())
        ).asType();
    }

    public ResourceIterator<Map<Reference, Vertex<?, ?>>> execute(GraphManager graphMgr, Traversal.Parameters parameters) {
        return iterate(Collections.emptyIterator()); // TODO
    }
}
