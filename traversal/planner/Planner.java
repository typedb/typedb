/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal.planner;

import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.common.Modifiers;
import com.vaticle.typedb.core.traversal.procedure.PermutationProcedure;
import com.vaticle.typedb.core.traversal.structure.Structure;

import java.util.List;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public interface Planner {

    static Planner create(Structure structure, Modifiers modifiers) {
        List<Structure> structures = splitStructure(structure, modifiers);
        if (structures.size() == 1) return ComponentPlanner.create(structures.get(0), modifiers);
        else return MultiPlanner.create(structures, modifiers);
    }

    /**
     * Split the structure into subgraphs, while maintaining semantics required for sorting
     * Additionally, optimise away subgraphs that are not included in the filter
     */
    static List<Structure> splitStructure(Structure structure, Modifiers modifiers) {
        return iterate(structure.splitDisjoint(modifiers.sorting().variables())).filter(s ->
                iterate(s.vertices()).anyMatch(v -> v.id().isRetrievable() && modifiers.filter().variables().contains(v.id().asVariable().asRetrievable()))
        ).toList();
    }

    PermutationProcedure procedure();

    boolean isOptimal();

    void tryOptimise(GraphManager graphMgr, boolean singleUse);
}
