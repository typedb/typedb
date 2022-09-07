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

package com.vaticle.typedb.core.traversal.planner;

import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.procedure.VertexProcedure;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;

public class VertexPlanner implements ConnectedPlanner {

    private final StructureVertex<?> structureVertex;
    private final VertexProcedure procedure;

    private VertexPlanner(StructureVertex<?> structureVertex) {
        this.structureVertex = structureVertex;
        this.procedure = VertexProcedure.create(structureVertex);
    }

    static VertexPlanner create(StructureVertex<?> structureVertex) {
        return new VertexPlanner(structureVertex);
    }

    @Override
    public VertexProcedure procedure() {
        return procedure;
    }

    public StructureVertex<?> structureVertex() {
        return structureVertex;
    }

    @Override
    public void tryOptimise(GraphManager graphMgr, boolean singleUse) {
        assert this.procedure != null;
    }

    @Override
    public boolean isVertex() {
        return true;
    }

    @Override
    public VertexPlanner asVertex() {
        return this;
    }

    @Override
    public boolean isOptimal() {
        return true;
    }
}
