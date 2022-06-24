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

import com.vaticle.typedb.core.traversal.procedure.VertexProcedure;
import com.vaticle.typedb.core.traversal.structure.Structure;

public class VertexPlanner implements Planner {

    private final VertexProcedure procedure;

    private VertexPlanner(VertexProcedure procedure) {
        this.procedure = procedure;
    }

    static VertexPlanner create(Structure structure) {
        assert structure.vertices().size() == 1;
        return new VertexPlanner(VertexProcedure.create(structure.vertices().iterator().next()));
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

    @Override
    public boolean isOptimal() {
        return true;
    }
}
