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

package com.vaticle.typedb.core.traversal.procedure;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;

public class TypeCombinationProcedure {

    private final GraphTraversal.Type traversal;
    private final Map<ProcedureVertex.Type, Set<ProcedureEdge<?, ?>>> forwardEdges;
    private final Map<ProcedureVertex.Type, Set<ProcedureEdge<?, ?>>> reverseEdges;
    private ProcedureVertex.Type startVertex;

    TypeCombinationProcedure(GraphTraversal.Type traversal) {
        this.traversal = traversal;
        this.forwardEdges = new HashMap<>();
        this.reverseEdges = new HashMap<>();
        computeForwardBackward();
    }

    private void computeForwardBackward() {
        Set<StructureEdge<?, ?>> visitedEdges = new HashSet<>();
        GraphProcedure.Builder builder = GraphProcedure.builder();
        startVertex = visitBfs(traversal.structure().vertices().iterator().next().asType(), visitedEdges, builder);
    }

    public static TypeCombinationProcedure of(GraphTraversal.Type traversal) {
        return new TypeCombinationProcedure(traversal);
    }

    public ProcedureVertex.Type startVertex() {
        return startVertex;
    }

    public boolean nonTerminal(ProcedureVertex.Type vertex) {
        return forwardEdges.containsKey(vertex);
    }

    public Set<ProcedureVertex.Type> terminals() {
        Set<ProcedureVertex.Type> terminals = new HashSet<>(reverseEdges.keySet());
        terminals.removeAll(forwardEdges.keySet());
        return terminals;
    }

    public Set<ProcedureEdge<?, ?>> forwardEdges(ProcedureVertex.Type vertex) {
        return forwardEdges.get(vertex);
    }

    public Set<ProcedureEdge<?, ?>> reverseEdges(ProcedureVertex.Type vertex) {
        return reverseEdges.get(vertex);
    }

    private ProcedureVertex.Type visitBfs(StructureVertex.Type structureVertex, Set<StructureEdge<?, ?>> visitedEdges,
                                          GraphProcedure.Builder builder) {
        boolean isStart = builder.vertices().size() == 0;
        ProcedureVertex.Type procedureVertex = vertex(structureVertex, isStart, builder);
        Set<StructureVertex.Type> next = visitOut(procedureVertex, structureVertex, builder, visitedEdges);
        next.addAll(visitIn(procedureVertex, structureVertex, builder, visitedEdges));
        next.forEach(vertex -> visitBfs(vertex, visitedEdges, builder));
        return procedureVertex;
    }

    private ProcedureVertex.Type vertex(StructureVertex.Type structureVertex, boolean isStart, GraphProcedure.Builder builder) {
        if (builder.containsVertex(structureVertex.id())) return builder.getType(structureVertex.id());
        else {
            ProcedureVertex.Type type = builder.type(structureVertex.id(), isStart);
            type.props(structureVertex.props());
            return type;
        }
    }

    private Set<StructureVertex.Type> visitOut(ProcedureVertex.Type procedureVertex, StructureVertex.Type structureVertex,
                          GraphProcedure.Builder builder, Set<StructureEdge<?, ?>> visitedEdges) {
        Set<StructureVertex.Type> next = new HashSet<>();
        if (!structureVertex.outs().isEmpty()) {
            structureVertex.outs().forEach(structureEdge -> {
                if (!visitedEdges.contains(structureEdge)) {
                    visitedEdges.add(structureEdge);
                    int order = visitedEdges.size();
                    ProcedureVertex.Type end = vertex(structureEdge.to().asType(), false, builder);
                    ProcedureEdge<?, ?> edge = createOut(procedureVertex, end, structureEdge, order, builder);
                    forwardEdges.computeIfAbsent(procedureVertex, (v) -> new HashSet<>()).add(edge);
                    reverseEdges.computeIfAbsent(end, (v) -> new HashSet<>()).add(edge.reverse());
                    next.add(structureEdge.to().asType());
                }
            });
        }
        return next;
    }

    private Set<StructureVertex.Type> visitIn(ProcedureVertex.Type procedureVertex, StructureVertex.Type structureVertex,
                                              GraphProcedure.Builder builder, Set<StructureEdge<?, ?>> visitedEdges) {
        Set<StructureVertex.Type> next = new HashSet<>();
        if (!structureVertex.ins().isEmpty()) {
            structureVertex.ins().forEach(structureEdge -> {
                if (!visitedEdges.contains(structureEdge)) {
                    visitedEdges.add(structureEdge);
                    int order = visitedEdges.size();
                    ProcedureVertex.Type start = vertex(structureEdge.from().asType(), false, builder);
                    ProcedureEdge<?, ?> edge = createIn(procedureVertex, start, structureEdge, order, builder);
                    forwardEdges.computeIfAbsent(procedureVertex, (v1) -> new HashSet<>()).add(edge);
                    reverseEdges.computeIfAbsent(start, (v) -> new HashSet<>()).add(edge.reverse());
                    next.add(structureEdge.from().asType());
                }
            });
        }
        return next;
    }

    private ProcedureEdge<?, ?> createOut(ProcedureVertex.Type from, ProcedureVertex.Type to, StructureEdge<?, ?> structureEdge,
                           int order, GraphProcedure.Builder builder) {
        if (structureEdge.isNative()) {
            switch (structureEdge.asNative().encoding().asType()) {
                case SUB:
                    return builder.forwardSub(order, from, to, structureEdge.asNative().isTransitive());
                case OWNS:
                    return builder.forwardOwns(order, from, to, false);
                case OWNS_KEY:
                    return builder.forwardOwns(order, from, to, true);
                case PLAYS:
                    return builder.forwardPlays(order, from, to);
                case RELATES:
                    return builder.forwardRelates(order, from, to);
                default:
                    throw TypeDBException.of(UNRECOGNISED_VALUE);
            }
        } else if (structureEdge.isEqual()) {
            return builder.forwardEqual(order, from, to);
        } else throw TypeDBException.of(ILLEGAL_STATE);
    }

    private ProcedureEdge<?, ?> createIn(ProcedureVertex.Type from, ProcedureVertex.Type to, StructureEdge<?, ?> structureEdge, int order, GraphProcedure.Builder builder) {
        if (structureEdge.isNative()) {
            switch (structureEdge.asNative().encoding().asType()) {
                case SUB:
                    return builder.backwardSub(order, from, to, structureEdge.asNative().isTransitive());
                case OWNS:
                    return builder.backwardOwns(order, from, to, false);
                case OWNS_KEY:
                    return builder.backwardOwns(order, from, to, true);
                case PLAYS:
                    return builder.backwardPlays(order, from, to);
                case RELATES:
                    return builder.backwardRelates(order, from, to);
                default:
                    throw TypeDBException.of(UNRECOGNISED_VALUE);
            }
        } else if (structureEdge.isEqual()) {
            return builder.backwardEqual(order, from, to);
        } else throw TypeDBException.of(ILLEGAL_STATE);
    }
}
