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

package com.vaticle.typedb.core.traversal.test;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;
import com.vaticle.typedb.core.traversal.structure.Structure;
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

        return iterate(permutations(retrievables)).map(idPermutation -> {
            Map<Identifier, Integer> orderingMap = new HashMap<>();
            labels.forEach(labelled -> orderingMap.put(labelled, orderingMap.size()));
            idPermutation.forEach(retrievable -> orderingMap.put(retrievable, orderingMap.size()));
            return GraphProcedure.create(structure, orderingMap);
        });
    }
}
