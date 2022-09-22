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

package com.vaticle.typedb.core.traversal.scanner;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typedb.core.traversal.common.Modifiers;
import com.vaticle.typedb.core.traversal.procedure.CombinationProcedure;
import com.vaticle.typedb.core.traversal.procedure.ProcedureEdge;
import com.vaticle.typedb.core.traversal.procedure.ProcedureVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;

public class CombinationFinder {

    private static final Logger LOG = LoggerFactory.getLogger(CombinationFinder.class);

    private final GraphManager graphMgr;
    private final CombinationProcedure procedure;
    private final Traversal.Parameters params;
    private final Modifiers.Filter filter;
    private final Set<Retrievable> concreteVarIds;
    private final Map<Identifier, Set<TypeVertex>> combination;

    private enum State {CHANGED, UNCHANGED, EMPTY}

    public CombinationFinder(GraphManager graphMgr, CombinationProcedure procedure, Modifiers.Filter filter,
                             Set<Retrievable> concreteVarIds) {
        assert filter.variables().containsAll(concreteVarIds);
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.filter = filter;
        this.concreteVarIds = concreteVarIds;
        this.params = new Traversal.Parameters();
        this.combination = new HashMap<>();
    }

    public Optional<Map<Retrievable, Set<TypeVertex>>> combination() {
        if (LOG.isTraceEnabled()) LOG.trace(procedure.toString());

        start(procedure);
        State state = State.CHANGED;
        while (state == State.CHANGED) {
            state = forward(procedure);
            if (state == State.EMPTY) return Optional.empty();
            state = backward(procedure);
            if (state == State.EMPTY) return Optional.empty();
        }
        return Optional.of(filtered(combination));
    }

    private void start(CombinationProcedure procedure) {
        procedure.startVertices().forEach(start -> addOrIntersect(start.id(), vertexIter(start).toSet()));
    }

    private State forward(CombinationProcedure procedure) {
        Queue<ProcedureVertex.Type> toVisit = new LinkedList<>();
        toVisit.addAll(procedure.startVertices());
        ProcedureVertex.Type from;
        boolean changed = false;
        while (!toVisit.isEmpty()) {
            from = toVisit.remove();
            changed = includeLoopCombinations(procedure, from) || changed;
            for (ProcedureEdge<?, ?> procedureEdge : procedure.forwardEdges(from)) {
                Set<TypeVertex> toCombination = toTypes(procedureEdge);
                changed = addOrIntersect(procedureEdge.to().id(), toCombination) || changed;
                if (combination.get(procedureEdge.to().id()).isEmpty()) return State.EMPTY;
                if (!procedure.isTerminal(procedureEdge.to().asType())) {
                    toVisit.add(procedureEdge.to().asType());
                }
            }
        }
        return changed ? State.CHANGED : State.UNCHANGED;
    }

    private State backward(CombinationProcedure procedure) {
        Queue<ProcedureVertex.Type> toVisit = new LinkedList<>(procedure.terminals());
        ProcedureVertex.Type from;
        boolean changed = false;
        while (!toVisit.isEmpty()) {
            from = toVisit.remove();
            changed = includeLoopCombinations(procedure, from) || changed;
            for (ProcedureEdge<?, ?> procedureEdge : procedure.reverseEdges(from)) {
                Set<TypeVertex> toTypes = toTypes(procedureEdge);
                changed = addOrIntersect(procedureEdge.to().id(), toTypes) || changed;
                if (combination.get(procedureEdge.to().id()).isEmpty()) return State.EMPTY;
                if (!procedure.startVertices().contains(procedureEdge.to().asType())) {
                    toVisit.add(procedureEdge.to().asType());
                }
            }
        }
        return changed ? State.CHANGED : State.UNCHANGED;
    }

    private boolean includeLoopCombinations(CombinationProcedure procedure, ProcedureVertex.Type from) {
        boolean changed = false;
        for (ProcedureEdge<?, ?> loopEdge : procedure.loopEdges(from)) {
            Set<TypeVertex> loopCombination = loopTypes(loopEdge);
            changed = addOrIntersect(loopEdge.from().id(), loopCombination) || changed;
        }
        return changed;
    }

    private Set<TypeVertex> loopTypes(ProcedureEdge<?, ?> edge) {
        assert edge.from().equals(edge.to());
        return iterate(combination.get(edge.from().id())).flatMap(type -> branchIter(edge, type).filter(to -> to.equals(type))).toSet();
    }

    private Set<TypeVertex> toTypes(ProcedureEdge<?, ?> edge) {
        assert !edge.from().equals(edge.to());
        return iterate(combination.get(edge.from().id())).flatMap(type -> branchIter(edge, type)).toSet();
    }

    private boolean addOrIntersect(Identifier identifier, Set<? extends TypeVertex> types) {
        Set<TypeVertex> vertices = combination.computeIfAbsent(identifier, (id) -> new HashSet<>());
        int sizeBefore = vertices.size();
        if (vertices.isEmpty()) vertices.addAll(types);
        else vertices.retainAll(types);
        return vertices.size() != sizeBefore;
    }

    private FunctionalIterator<? extends TypeVertex> vertexIter(ProcedureVertex.Type vertex) {
        FunctionalIterator<? extends TypeVertex> iterator = vertex.iterator(graphMgr, params, ASC);
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
            if (id.isRetrievable() && filter.variables().contains(id.asVariable().asRetrievable())) {
                filtered.put(id.asVariable().asRetrievable(), vertices);
            }
        });
        return filtered;
    }

}
