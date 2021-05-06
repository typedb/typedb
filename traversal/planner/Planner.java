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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.procedure.Procedure;
import com.vaticle.typedb.core.traversal.structure.Structure;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public interface Planner {

    Procedure procedure();

    default void tryOptimise(GraphManager graphMgr, boolean extraTime) {
        if (isGraph()) this.asGraph().mayOptimise(graphMgr, extraTime);
    }

    static Planner create(Structure structure) {
        if (structure.vertices().size() == 1) return VertexPlanner.create(structure);
        else return GraphPlanner.create(structure);
    }

    default boolean isVertex() { return false; }

    default boolean isGraph() { return false; }

    default VertexPlanner asVertex() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(VertexPlanner.class));
    }

    default GraphPlanner asGraph() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(GraphPlanner.class));
    }
}
