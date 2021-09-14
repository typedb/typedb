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

package com.vaticle.typedb.core.traversal.scanner;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typedb.core.traversal.procedure.CombinationProcedure;
import com.vaticle.typedb.core.traversal.procedure.ProcedureEdge;
import com.vaticle.typedb.core.traversal.procedure.ProcedureVertex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class CombinationFinder {

    private final GraphManager graphMgr;
    private final Set<CombinationProcedure> procedures;
    private final Traversal.Parameters params;
    private final Map<Identifier, Set<TypeVertex>> combination;
    private final Set<Retrievable> filter;
    private final Set<Retrievable> concreteVarIds;

    private enum Status {CHANGED, UNCHANGED, EMPTY}

    private CombinationFinder(GraphManager graphMgr, Set<CombinationProcedure> procedures, Set<Retrievable> filter,
                              Set<Retrievable> concreteVarIds) {
        assert filter.containsAll(concreteVarIds);
        this.graphMgr = graphMgr;
        this.procedures = procedures;
        this.params = new Traversal.Parameters();
        this.filter = filter;
        this.concreteVarIds = concreteVarIds;
        this.combination = new HashMap<>();
    }

    public static Optional<Map<Retrievable, Set<TypeVertex>>> find(GraphManager graphMgr, Set<CombinationProcedure> procedures,
                                                                   Set<Retrievable> filter, Set<Retrievable> concreteTypesOnly) {
        return new CombinationFinder(graphMgr, procedures, filter, concreteTypesOnly).combination();
    }

    private Optional<Map<Retrievable, Set<TypeVertex>>> combination() {
        for (CombinationProcedure procedure : procedures) {
            initialise(procedure);
            if (procedure.vertices().size() == 1) continue;
            Status status = Status.CHANGED;
            while (status == Status.CHANGED) {
                status = forward(procedure);
                if (status == Status.EMPTY) return Optional.empty();
                status = backward(procedure);
                if (status == Status.EMPTY) return Optional.empty();
            }
        }
        return Optional.of(filtered(combination));
    }

    private void initialise(CombinationProcedure procedure) {
        ProcedureVertex.Type from = procedure.startVertex();
        recordCombination(from.id(), vertexIter(from).toSet());
    }

    private Status forward(CombinationProcedure procedure) {
        Queue<ProcedureVertex.Type> toVisit = new LinkedList<>();
        toVisit.add(procedure.startVertex());
        ProcedureVertex.Type from;
        boolean changed = false;
        while (!toVisit.isEmpty()) {
            from = toVisit.remove();
            for (ProcedureEdge<?, ?> procedureEdge : procedure.forwardEdges(from)) {
                Set<TypeVertex> toCombination = toCombination(procedureEdge);
                changed = recordCombination(procedureEdge.to().id(), toCombination) || changed;
                if (combination.get(procedureEdge.to().id()).isEmpty()) return Status.EMPTY;
                if (!procedure.isTerminal(procedureEdge.to().asType()) && !from.equals(procedureEdge.to())) {
                    toVisit.add(procedureEdge.to().asType());
                }
            }
        }
        return changed ? Status.CHANGED : Status.UNCHANGED;
    }

    private Status backward(CombinationProcedure procedure) {
        Queue<ProcedureVertex.Type> toVisit = new LinkedList<>(procedure.terminals());
        ProcedureVertex.Type from;
        boolean changed = false;
        while (!toVisit.isEmpty()) {
            from = toVisit.remove();
            for (ProcedureEdge<?, ?> procedureEdge : procedure.reverseEdges(from)) {
                Set<TypeVertex> toTypes = toCombination(procedureEdge);
                changed = recordCombination(procedureEdge.to().id(), toTypes) || changed;
                if (combination.get(procedureEdge.to().id()).isEmpty()) return Status.EMPTY;
                if (!procedureEdge.to().isStartingVertex()) toVisit.add(procedureEdge.to().asType());
            }
        }
        return changed ? Status.CHANGED : Status.UNCHANGED;
    }

    private Set<TypeVertex> toCombination(ProcedureEdge<?, ?> edge) {
        return iterate(combination.get(edge.from().id())).flatMap(type -> branchIter(edge, type)).toSet();
    }

    private boolean recordCombination(Identifier identifier, Set<TypeVertex> types) {
        Set<TypeVertex> vertices = combination.computeIfAbsent(identifier, (id) -> new HashSet<>());
        int sizeBefore = vertices.size();
        if (vertices.isEmpty()) vertices.addAll(types);
        else vertices.retainAll(types);
        return vertices.size() != sizeBefore;
    }

    private FunctionalIterator<TypeVertex> vertexIter(ProcedureVertex.Type vertex) {
        FunctionalIterator<TypeVertex> iterator = vertex.iterator(graphMgr, params);
        if (vertex.id().isRetrievable() && concreteVarIds.contains(vertex.id().asVariable().asRetrievable())) {
            iterator = iterator.filter(type -> !type.isAbstract());
        }
        return iterator;
    }

    private FunctionalIterator<TypeVertex> branchIter(ProcedureEdge<?, ?> edge, TypeVertex vertex) {
        FunctionalIterator<TypeVertex> iterator = edge.branch(graphMgr, vertex, params).map(Vertex::asType);
        if (edge.to().id().isRetrievable() && concreteVarIds.contains(edge.to().id().asVariable().asRetrievable())) {
            iterator = iterator.filter(type -> !type.isAbstract());
        }
        return iterator;
    }

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
