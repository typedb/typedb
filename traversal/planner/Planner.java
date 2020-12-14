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

package grakn.core.traversal.planner;

import grakn.core.common.exception.GraknException;
import grakn.core.graph.GraphManager;
import grakn.core.traversal.procedure.Procedure;
import grakn.core.traversal.structure.Structure;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public interface Planner {

    Procedure procedure();

    default void tryOptimise(GraphManager graphMgr) {
        if (isGraph()) this.asGraph().optimise(graphMgr);
    }

    static Planner create(Structure structure) {
        if (structure.edges().isEmpty()) return VertexPlanner.create(structure);
        else return GraphPlanner.create(structure);
    }

    default boolean isVertex() { return false; }

    default boolean isGraph() { return false; }

    default VertexPlanner asVertex() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(VertexPlanner.class));
    }

    default GraphPlanner asGraph() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(GraphPlanner.class));
    }
}
