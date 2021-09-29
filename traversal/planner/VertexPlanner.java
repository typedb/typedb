/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.traversal.planner;

import com.vaticle.typedb.core.traversal.procedure.ProcedureEdge;
import com.vaticle.typedb.core.traversal.procedure.ProcedureVertex;
import com.vaticle.typedb.core.traversal.procedure.VertexProcedure;
import com.vaticle.typedb.core.traversal.structure.Structure;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;

public class VertexPlanner implements Planner {

    private final VertexProcedure procedure;

    private VertexPlanner(VertexProcedure procedure) {
        this.procedure = procedure;
    }

    static VertexPlanner create(Structure structure) {
        assert structure.vertices().size() == 1;
        ProcedureVertex<?, ?> plannerVertex = toProcedure(structure.vertices().iterator().next());
        VertexProcedure proc = new VertexProcedure(plannerVertex);
        return new VertexPlanner(proc);
    }

    private static ProcedureVertex<?, ?> toProcedure(StructureVertex<?> structureVertex) {

        ProcedureVertex<?, ?> vertex = structureVertex.isType() ? new ProcedureVertex.Type(structureVertex.id(), true)
                : new ProcedureVertex.Thing(structureVertex.id(), true);

        if (vertex.isType()) vertex.asType().props(structureVertex.asType().props());
        else vertex.asThing().props(structureVertex.asThing().props());

        int order = 0;
        for (StructureEdge<?, ?> structureEdge : structureVertex.outs()) {
            ProcedureEdge<?, ?> edge = ProcedureEdge.of(vertex, vertex, structureEdge, order, true);
            vertex.out(edge);
        }
        return vertex;
    }

    @Override
    public VertexProcedure procedure() {
        return procedure;
    }

    @Override
    public boolean isVertex() {
        return true;
    }

    @Override
    public VertexPlanner asVertex() {
        return this;
    }
}
