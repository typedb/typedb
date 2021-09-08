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
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typedb.core.traversal.procedure.ProcedureEdge;
import com.vaticle.typedb.core.traversal.procedure.ProcedureVertex;
import com.vaticle.typedb.core.traversal.procedure.TypeCombinationProcedure;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class TypeCombinationGetter {

    private final GraphManager graphMgr;
    private final TypeCombinationProcedure procedure;
    private final Traversal.Parameters params;
    private final Map<Identifier, Set<TypeVertex>> combination;
    private final Set<Retrievable> filter;
    private final Set<Retrievable> concreteTypesOnly;

    private TypeCombinationGetter(GraphManager graphMgr, TypeCombinationProcedure procedure, Traversal.Parameters params,
                                  Set<Retrievable> filter, Set<Retrievable> concreteTypesOnly) {
        assert filter.containsAll(concreteTypesOnly);
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.params = params;
        this.filter = filter;
        this.concreteTypesOnly = concreteTypesOnly;
        this.combination = new HashMap<>();
    }

    public static Optional<Map<Retrievable, Set<TypeVertex>>> get(GraphManager graphMgr, TypeCombinationProcedure procedure,
                                                                  Traversal.Parameters parameters, Set<Retrievable> filter,
                                                                  Set<Retrievable> concreteTypesOnly) {
        return new TypeCombinationGetter(graphMgr, procedure, parameters, filter, concreteTypesOnly).combination();
    }

    public Optional<Map<Retrievable, Set<TypeVertex>>> combination() {
        if (!forward()) return Optional.empty();
        if (!backward()) return Optional.empty();
        return Optional.of(filtered(combination));
    }

    private boolean forward() {
        Queue<ProcedureVertex.Type> vertices = new LinkedList<>();
        ProcedureVertex.Type from = procedure.startVertex();
        record(from.id(), vertexIter(from).toSet());
        vertices.add(from);
        while (!vertices.isEmpty()) {
            from = vertices.remove();
            for (ProcedureEdge<?, ?> procedureEdge : procedure.forwardEdges(from)) {
                assert combination.containsKey(from.id()) && procedureEdge.from().id().equals(from.id());
                Set<TypeVertex> toTypes = new HashSet<>();
                for (TypeVertex type : combination.get(from.id())) {
                    branchIter(procedureEdge, type).forEachRemaining(toTypes::add);
                }
                record(procedureEdge.to().id(), toTypes);
                if (combination.get(procedureEdge.to().id()).isEmpty()) return false;
                if (procedure.hasEdges(procedureEdge.to().asType())) vertices.add(procedureEdge.to().asType());
            }
        }
        return true;
    }

    private boolean backward() {
        Queue<ProcedureVertex.Type> vertices = new LinkedList<>(procedure.terminals());
        ProcedureVertex.Type from;
        while (!vertices.isEmpty()) {
            from = vertices.remove();
            for (ProcedureEdge<?, ?> procedureEdge : procedure.reverseEdges(from)) {
                assert combination.containsKey(procedureEdge.from().id()) && combination.containsKey(from.id())
                        && procedureEdge.from().id().equals(from.id());
                Set<TypeVertex> toTypes = new HashSet<>();
                for (TypeVertex type : combination.get(from.id())) {
                    branchIter(procedureEdge, type).forEachRemaining(toTypes::add);
                }
                record(procedureEdge.to().id(), toTypes);
                if (combination.get(procedureEdge.to().id()).isEmpty()) return false;
                if (!procedureEdge.to().isStartingVertex()) vertices.add(procedureEdge.to().asType());
            }
        }
        return true;
    }

    private void record(Identifier identifier, Set<TypeVertex> types) {
        Set<TypeVertex> vertices = combination.computeIfAbsent(identifier, (id) -> new HashSet<>());
        if (vertices.isEmpty()) vertices.addAll(types);
        else vertices.retainAll(types);
    }

    private FunctionalIterator<TypeVertex> vertexIter(ProcedureVertex.Type vertex) {
        FunctionalIterator<TypeVertex> iterator = vertex.iterator(graphMgr, params);
        if (vertex.id().isRetrievable() && concreteTypesOnly.contains(vertex.id().asVariable().asRetrievable())) {
            iterator = iterator.filter(type -> !type.isAbstract());
        }
        return iterator;
    }

    private FunctionalIterator<TypeVertex> branchIter(ProcedureEdge<?, ?> edge, TypeVertex vertex) {
        FunctionalIterator<TypeVertex> iterator = edge.branch(graphMgr, vertex, params).map(Vertex::asType);
        if (edge.to().id().isRetrievable() && concreteTypesOnly.contains(edge.to().id().asVariable().asRetrievable())) {
           iterator = iterator.filter(type -> !type.isAbstract());
        }
        return iterator;
    }


    //        assert procedure.proceduresCount() > 0;
//        TypeCombinationProcedure.VertexCombinationProcedure combinationProcedure = procedure.first();
//        evaluateCombination(combinationProcedure.permutationProcedure());
//        if (!combination.containsKey(combinationProcedure.vertexId())) return Optional.empty();
//        for (int i = 1; i < procedure.proceduresCount(); i++) {
//            combinationProcedure = procedure.next(combinationProcedure, combination.get(combinationProcedure.vertexId()));
//            evaluateCombination(combinationProcedure.permutationProcedure());
//            if (!combination.containsKey(combinationProcedure.vertexId())) return Optional.empty();
//        }
//        return Optional.of(filtered(combination));
//    }
//
//    private void evaluateCombination(GraphProcedure procedure) {
//        if (procedure.edgesCount() == 0) evaluateVertex(procedure.startVertex().asType());
//        else evaluateGraph(procedure);
//    }
//
//    private void evaluateVertex(ProcedureVertex.Type vertex) {
//        assert vertex.id().isRetrievable();
//        FunctionalIterator<TypeVertex> iterator = vertex.iterator(graphMgr, params);
//        if (concreteTypesOnly.contains(vertex.id().asVariable().asRetrievable())) {
//            iterator = iterator.filter(type -> !type.isAbstract());
//        }
//        if (iterator.hasNext()) {
//            Set<TypeVertex> types = combination.computeIfAbsent(vertex.id(), i -> new HashSet<>());
//            iterator.forEachRemaining(types::add);
//        }
//    }
//
//    private void evaluateGraph(GraphProcedure procedure) {
//        ProcedureVertex.Type start = procedure.startVertex().asType();
//        boolean combinationPartiallyEvaluated = combination.containsKey(start.id());
//        recordCombination(start.iterator(graphMgr, params)
//                .filter(typeVertex -> !combinationPartiallyEvaluated || !combination.get(start.id()).contains(typeVertex))
//                .map(vertex -> new GraphIterator(graphMgr, vertex, procedure, params, filter)
//                        .filter(vertexMap -> !containsDisallowedAbstract(vertexMap)).first()
//                ).filter(Optional::isPresent).map(Optional::get));
//    }
//
//    private void recordCombination(FunctionalIterator<VertexMap> combinationIter) {
//        combinationIter.forEachRemaining(vertexMap -> vertexMap.forEach((id, vertex) -> {
//            Set<TypeVertex> types = combination.computeIfAbsent(id, i -> new HashSet<>());
//            types.add(vertex.asType());
//        }));
//    }
//
//    private boolean containsDisallowedAbstract(VertexMap vertexMap) {
//        return iterate(vertexMap.map().entrySet())
//                .anyMatch(entry -> concreteTypesOnly.contains(entry.getKey()) && entry.getValue().asType().isAbstract());
//    }
//
    private Map<Retrievable, Set<TypeVertex>> filtered(Map<Identifier, Set<TypeVertex>> answer) {
        Map<Retrievable, Set<TypeVertex>> filtered = new HashMap<>();
        answer.forEach((id, vertices) -> {
            if (id.isRetrievable() && filter.contains(id.asVariable().asRetrievable())) {
                filtered.put(id.asVariable().asRetrievable(), vertices);
            }
        });
        return filtered;
    }

}
