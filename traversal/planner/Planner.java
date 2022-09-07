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
import com.vaticle.typedb.core.traversal.procedure.PermutationProcedure;
import com.vaticle.typedb.core.traversal.structure.Structure;

import java.util.List;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public interface Planner {

    static Planner create(Structure structure) {
        List<Structure> retrievedStructures = retrievedStructures(structure.asGraphs());
        if (retrievedStructures.size() == 1) return ConnectedPlanner.create(retrievedStructures.get(0));
        else return MultiPlanner.create(retrievedStructures);
    }

    /**
     * Filter out structures that are not retrievable, such as disconnected type vertices
     * TODO: We can further improve this if we have access to the filter as well
     */
    static List<Structure> retrievedStructures(List<Structure> structures) {
        return iterate(structures).filter(s ->
                iterate(s.vertices()).anyMatch(v -> v.id().isRetrievable())
        ).toList();
    }

    PermutationProcedure procedure();

    boolean isOptimal();

    void tryOptimise(GraphManager graphMgr, boolean singleUse);
}
