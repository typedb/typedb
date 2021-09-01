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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;
import com.vaticle.typedb.core.traversal.procedure.ProcedureVertex;
import com.vaticle.typedb.core.traversal.procedure.TypeCombinationProcedure;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class TypeCombination {

    private final GraphManager graphMgr;
    private final TypeCombinationProcedure procedure;
    private final Traversal.Parameters params;
    private final Map<Identifier, Set<TypeVertex>> answer;
    private final Set<Identifier.Variable.Retrievable> filter;
    private final Set<Identifier.Variable.Retrievable> abstractDisallowed;

    public TypeCombination(GraphManager graphMgr, TypeCombinationProcedure procedure, Traversal.Parameters params,
                           Set<Identifier.Variable.Retrievable> filter, Set<Identifier.Variable.Retrievable> abstractDisallowed) {
        assert filter.containsAll(abstractDisallowed);
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.params = params;
        this.filter = filter;
        this.abstractDisallowed = abstractDisallowed;
        this.answer = new HashMap<>();
    }

    public Optional<Map<Identifier.Variable.Retrievable, Set<TypeVertex>>> get() {
        assert procedure.evaluationVertexCount() > 0;
        TypeCombinationProcedure.VertexEvaluation evaluation = procedure.begin();
        Set<TypeVertex> types = getTypes(evaluation.procedure());
        if (types.isEmpty()) return Optional.empty();
        answer.put(evaluation.evaluationId(), types);
        for (int i = 1; i < procedure.evaluationVertexCount(); i++) {
            evaluation = evaluation.next(types);
            types = getTypes(evaluation.procedure());
            if (types.isEmpty()) return Optional.empty();
            answer.put(evaluation.evaluationId(), types);
        }
        return Optional.of(filtered(answer));
    }

    private Set<TypeVertex> getTypes(GraphProcedure procedure) {
        assert procedure.startVertex().isType();
        ProcedureVertex.Type start = procedure.startVertex().asType();
        if (this.procedure.traversalVertexCount() == 1) {
            assert start.id().isRetrievable();
            FunctionalIterator<TypeVertex> iterator = start.iterator(graphMgr, params);
            if (abstractDisallowed.contains(start.id().asVariable().asRetrievable())) {
                iterator = iterator.filter(type -> !type.isAbstract());
            }
            return iterator.toSet();
        } else {
            return start.iterator(graphMgr, params).filter(vertex ->
                    new GraphIterator(graphMgr, vertex, procedure, params, filter)
                            .filter(vertexMap -> !containsDisallowedAbstract(vertexMap)).first().isPresent()).toSet();
        }
    }

    private boolean containsDisallowedAbstract(VertexMap vertexMap) {
        return iterate(vertexMap.map().entrySet())
                .anyMatch(entry -> abstractDisallowed.contains(entry.getKey()) && entry.getValue().asType().isAbstract());
    }

    private Map<Identifier.Variable.Retrievable, Set<TypeVertex>> filtered(Map<Identifier, Set<TypeVertex>> answer) {
        Map<Identifier.Variable.Retrievable, Set<TypeVertex>> filtered = new HashMap<>();
        answer.forEach((id, vertices) -> {
            if (id.isRetrievable() && filter.contains(id.asVariable().asRetrievable())) {
                filtered.put(id.asVariable().asRetrievable(), vertices);
            }
        });
        return filtered;
    }

}
