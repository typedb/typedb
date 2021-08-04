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

package com.vaticle.typedb.core.traversal.iterator;

import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.procedure.ProcedureEdge;
import com.vaticle.typedb.core.traversal.procedure.ProcedureVertex;
import com.vaticle.typedb.core.traversal.procedure.TypeCombinationProcedure;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TypeCombination {

    private final GraphManager graphMgr;
    private final TypeCombinationProcedure procedure;
    private final Traversal.Parameters params;
    private final Map<Identifier, Set<TypeVertex>> answer;

    public TypeCombination(GraphManager graphMgr, TypeCombinationProcedure procedure, Traversal.Parameters params) {
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.params = params;
        this.answer = new HashMap<>();
    }

    public Optional<Map<Identifier, Set<TypeVertex>>> get() {
        start(procedure.edge(0).from().asType());
        for (int pos = 0; pos < procedure.edgeSize(); pos++) {
            ProcedureEdge.Native.Type edge = procedure.edge(pos);
            if (edge.isClosureEdge()) closure(edge);
            else branch(edge);
            if (answer.get(edge.from().id()).isEmpty() || answer.get(edge.to().id()).isEmpty()) return Optional.empty();
        }
        return Optional.of(answer);
    }

    private void start(ProcedureVertex.Type vertex) {
        answer.put(vertex.id(), vertex.iterator(graphMgr, params).toSet());
    }

    private void branch(ProcedureEdge.Native.Type edge) {
        assert answer.containsKey(edge.from().id()) && !answer.containsKey(edge.to().id());
        Set<TypeVertex> to = answer.computeIfAbsent(edge.to().id(), id -> new HashSet<>());
        answer.get(edge.from().id()).forEach(typeVertex -> to.addAll(edge.branch(graphMgr, typeVertex, params).toList()));
    }

    private void closure(ProcedureEdge.Native.Type edge) {
        assert answer.containsKey(edge.from().id()) && answer.containsKey(edge.to().id());
        for (Iterator<TypeVertex> fromIter = answer.get(edge.from().id()).iterator(); fromIter.hasNext();) {
            TypeVertex from = fromIter.next();
            for (Iterator<TypeVertex> toIter = answer.get(edge.to().id()).iterator(); toIter.hasNext();) {
                TypeVertex to = toIter.next();
                if (!edge.isClosure(graphMgr, from, to, params)) {
                    fromIter.remove();
                    toIter.remove();
                }
            }
        }
    }
}
