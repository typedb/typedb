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
import com.vaticle.typedb.core.traversal.graph.TraversalVertex;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;
import com.vaticle.typedb.core.traversal.structure.Structure;

import java.util.HashMap;
import java.util.Map;

import static com.vaticle.typedb.common.collection.Collections.permutations;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class ProcedurePermutator {

    public static FunctionalIterator<GraphProcedure> generate(Structure structure) {
        return iterate(permutations(iterate(structure.vertices()).map(TraversalVertex::id).toSet()))
                .map(idPermutation -> {
                    Map<Identifier, Integer> orderingMap = new HashMap<>();
                    for (int index = 0; index < idPermutation.size(); index++) {
                        orderingMap.put(idPermutation.get(index), index);
                    }
                    return GraphProcedure.create(structure, orderingMap);
                });
    }
}
