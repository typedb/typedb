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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.graph.TraversalVertex;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;
import com.vaticle.typedb.core.traversal.structure.Structure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.vaticle.typedb.common.collection.Collections.permutations;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class ProcedurePermutator {

    // TODO: receive a Traversal object once the asGraphs call disappears
    public static List<GraphProcedure> generate(Structure structure) {
        if (structure.asGraphs().size() != 1) throw TypeDBException.of(ILLEGAL_ARGUMENT);
        List<GraphProcedure> procedures = new ArrayList<>();
        for (List<Identifier> ordering : permutations(iterate(structure.vertices()).map(TraversalVertex::id).toList())) {
            Map<Identifier, Integer> orderingMap = new HashMap<>();
            for (int index = 0; index < ordering.size(); index++) {
                orderingMap.put(ordering.get(index), index);
            }
            procedures.add(GraphProcedure.create(structure, orderingMap));
        }
        return procedures;
    }
}
