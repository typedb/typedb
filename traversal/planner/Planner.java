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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.procedure.PermutationProcedure;
import com.vaticle.typedb.core.traversal.structure.Structure;

import java.util.List;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public interface Planner {

    PermutationProcedure procedure();

    boolean isOptimal();

    default void tryOptimise(GraphManager graphMgr, boolean singleUse) {
        if (isGraph()) this.asGraph().mayOptimise(graphMgr, singleUse);
    }

    static Planner create(Structure structure) {
        List<Structure> retrievedGraphs = iterate(structure.asGraphs()).filter(s ->
            // we can eliminate subgraphs that are not retrievable
            // TODO elimination can further be improved if the planning includes the traversal filter
            iterate(s.vertices()).anyMatch(v -> v.id().isRetrievable())
        ).toList();
        if (retrievedGraphs.size() == 1 && retrievedGraphs.get(0).vertices().size() == 1) {
            return VertexPlanner.create(retrievedGraphs.get(0).vertices().iterator().next());
        } else return GraphPlanner.create(retrievedGraphs);
    }

    default boolean isVertex() {
        return false;
    }

    default boolean isGraph() {
        return false;
    }

    default VertexPlanner asVertex() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(VertexPlanner.class));
    }

    default GraphPlanner asGraph() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(GraphPlanner.class));
    }
}
