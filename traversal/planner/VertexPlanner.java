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

package grakn.core.traversal.planner;

import grakn.core.traversal.procedure.VertexProcedure;
import grakn.core.traversal.structure.Structure;
import grakn.core.traversal.structure.StructureEdge;
import grakn.core.traversal.structure.StructureVertex;

public class VertexPlanner implements Planner {

    private final VertexProcedure procedure;

    private VertexPlanner(VertexProcedure procedure) {
        this.procedure = procedure;
    }

    static VertexPlanner create(Structure structure) {
        assert structure.vertices().size() == 1;
        PlannerVertex<?> plannerVertex = toPlanner(structure.vertices().iterator().next());
        VertexProcedure proc = VertexProcedure.create(plannerVertex);
        return new VertexPlanner(proc);
    }

    private static PlannerVertex<?> toPlanner(StructureVertex<?> structureVertex) {
        PlannerVertex<?> plannerVertex = structureVertex.isType()
                ? new PlannerVertex.Type(structureVertex.id())
                : new PlannerVertex.Thing(structureVertex.id());
        if (plannerVertex.isType()) plannerVertex.asType().props(structureVertex.asType().props());
        else plannerVertex.asThing().props(structureVertex.asThing().props());
        plannerVertex.setStartingVertex();

        int order = 0;
        for (StructureEdge<?, ?> structureEdge : structureVertex.outs()) {
            PlannerEdge<?, ?> plannerEdge = PlannerEdge.of(plannerVertex, plannerVertex, structureEdge);
            plannerEdge.backward().setUnselected();
            plannerEdge.forward().setSelected();
            plannerEdge.forward().setOrder(++order);
            plannerVertex.out(plannerEdge);
            plannerVertex.in(plannerEdge);
        }
        if (!structureVertex.outs().isEmpty()) {
            plannerVertex.setHasOutGoingEdges();
            plannerVertex.setHasIncomingEdges();
        }

        return plannerVertex;
    }

    @Override
    public VertexProcedure procedure() {
        return procedure;
    }

    @Override
    public boolean isVertex() { return true; }

    @Override
    public VertexPlanner asVertex() { return this; }
}