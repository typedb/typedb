/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal.test;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;
import com.vaticle.typedb.core.traversal.structure.Structure;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Permutations.permutations;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class ProcedurePermutator {

    public static FunctionalIterator<GraphProcedure> generate(Structure structure) {
        // to reduce the permutation space, we always will put the type vertices at the start
        Set<Identifier> retrievables = new HashSet<>();
        List<Identifier.Variable.Label> labels = new ArrayList<>();
        for (StructureVertex<?> vertex : structure.vertices()) {
            if (vertex.id().isLabel()) labels.add(vertex.id().asVariable().asLabel());
            else retrievables.add(vertex.id());
        }

        return iterate(permutations(retrievables))
                .filter(idPermutation -> isValidPermutation(structure, idPermutation))
                .map(idPermutation -> {
                    Map<Identifier, Integer> orderingMap = new HashMap<>();
                    labels.forEach(labelled -> orderingMap.put(labelled, orderingMap.size()));
                    idPermutation.forEach(retrievable -> orderingMap.put(retrievable, orderingMap.size()));
                    return GraphProcedure.create(structure, orderingMap);
                });
    }

    private static boolean isValidPermutation(Structure structure, List<Identifier> idPermutation) {
        Set<Identifier> seen = new HashSet<>();
        Map<Identifier, StructureVertex<?>> vertices = new HashMap<>();
        structure.vertices().forEach(vertex -> vertices.put(vertex.id(), vertex));
        for (Identifier id : idPermutation) {
            StructureVertex<?> vertex = vertices.get(id);
            if (vertex.isValue()) {
                if (!iterate(vertex.ins()).filter(StructureEdge::isArgument).allMatch(argEdge -> seen.contains(argEdge.from().id()))) {
                    return false;
                }
            }
            seen.add(id);
        }
        return true;
    }
}
