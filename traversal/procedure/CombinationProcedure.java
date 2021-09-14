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
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.structure.Structure;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;

public class CombinationProcedure {

    private final Identifier startId;
    private final Map<Identifier, ProcedureVertex.Type> vertices;
    private final Map<ProcedureVertex.Type, Set<ProcedureEdge<?, ?>>> forwardEdges;
    private final Map<ProcedureVertex.Type, Set<ProcedureEdge<?, ?>>> reverseEdges;

    CombinationProcedure(Identifier startId) {
        this.startId = startId;
        this.vertices = new HashMap<>();
        this.forwardEdges = new HashMap<>();
        this.reverseEdges = new HashMap<>();
    }

    public static CombinationProcedure create(Structure structure) {
        StructureVertex.Type startVertex = structure.vertices().iterator().next().asType();
        CombinationProcedure combinationProcedure = new CombinationProcedure(startVertex.id());
        Set<StructureEdge<?, ?>> visitedEdges = new HashSet<>();
        combinationProcedure.registerBFS(startVertex, visitedEdges, true, combinationProcedure);
        return combinationProcedure;
    }

    public ProcedureVertex.Type startVertex() {
        return vertices.get(startId);
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
        assert forwardEdges.containsKey(vertex);
        return forwardEdges.get(vertex);
    }

    public Set<ProcedureEdge<?, ?>> reverseEdges(ProcedureVertex.Type vertex) {
        assert reverseEdges.containsKey(vertex);
        return reverseEdges.get(vertex);
    }

    private ProcedureVertex.Type vertex(StructureVertex.Type sv, boolean isStart) {
        return vertices.computeIfAbsent(sv.id(), id -> {
            ProcedureVertex.Type vertex = new ProcedureVertex.Type(id, isStart);
            vertex.props(sv.props());
            return vertex;
        });
    }

    private void registerBFS(StructureVertex.Type structureVertex, Set<StructureEdge<?, ?>> visitedEdges,
                             boolean isStart, CombinationProcedure combinationProcedure) {
        ProcedureVertex.Type procedureVertex = combinationProcedure.vertex(structureVertex, isStart);
        Set<StructureVertex.Type> next = registerOut(procedureVertex, structureVertex, visitedEdges, combinationProcedure);
        next.addAll(registerIn(procedureVertex, structureVertex, visitedEdges, combinationProcedure));
        next.forEach(vertex -> registerBFS(vertex, visitedEdges, false, combinationProcedure));
    }

    private Set<StructureVertex.Type> registerOut(ProcedureVertex.Type procedureVertex, StructureVertex.Type structureVertex,
                                                  Set<StructureEdge<?, ?>> visitedEdges, CombinationProcedure combinationProcedure) {
        Set<StructureVertex.Type> next = new HashSet<>();
        if (!structureVertex.outs().isEmpty()) {
            structureVertex.outs().forEach(structureEdge -> {
                if (!visitedEdges.contains(structureEdge)) {
                    visitedEdges.add(structureEdge);
                    int order = visitedEdges.size();
                    ProcedureVertex.Type end = combinationProcedure.vertex(structureEdge.to().asType(), false);
                    ProcedureEdge<?, ?> edge = createOut(procedureVertex, end, structureEdge, order);
                    combinationProcedure.forwardEdges.computeIfAbsent(procedureVertex, (v) -> new HashSet<>()).add(edge);
                    combinationProcedure.reverseEdges.computeIfAbsent(end, (v) -> new HashSet<>()).add(edge.reverse());
                    next.add(structureEdge.to().asType());
                }
            });
        }
        return next;
    }

    private Set<StructureVertex.Type> registerIn(ProcedureVertex.Type procedureVertex, StructureVertex.Type structureVertex,
                                                 Set<StructureEdge<?, ?>> visitedEdges, CombinationProcedure combinationProcedure) {
        Set<StructureVertex.Type> next = new HashSet<>();
        if (!structureVertex.ins().isEmpty()) {
            structureVertex.ins().forEach(structureEdge -> {
                if (!visitedEdges.contains(structureEdge)) {
                    visitedEdges.add(structureEdge);
                    int order = visitedEdges.size();
                    ProcedureVertex.Type start = combinationProcedure.vertex(structureEdge.from().asType(), false);
                    ProcedureEdge<?, ?> edge = createIn(procedureVertex, start, structureEdge, order);
                    combinationProcedure.forwardEdges.computeIfAbsent(procedureVertex, (v1) -> new HashSet<>()).add(edge);
                    combinationProcedure.reverseEdges.computeIfAbsent(start, (v) -> new HashSet<>()).add(edge.reverse());
                    next.add(structureEdge.from().asType());
                }
            });
        }
        return next;
    }

    private ProcedureEdge<?, ?> createOut(ProcedureVertex.Type from, ProcedureVertex.Type to, StructureEdge<?, ?> structureEdge,
                                          int order) {
        ProcedureEdge<?, ?> edge;
        if (structureEdge.isNative()) {
            switch (structureEdge.asNative().encoding().asType()) {
                case SUB:
                    edge = new ProcedureEdge.Native.Type.Sub.Forward(from, to, order, structureEdge.asNative().isTransitive());
                    break;
                case OWNS:
                    edge = new ProcedureEdge.Native.Type.Owns.Forward(from, to, order, false);
                    break;
                case OWNS_KEY:
                    edge = new ProcedureEdge.Native.Type.Owns.Forward(from, to, order, true);
                    break;
                case PLAYS:
                    edge = new ProcedureEdge.Native.Type.Plays.Forward(from, to, order);
                    break;
                case RELATES:
                    edge = new ProcedureEdge.Native.Type.Relates.Forward(from, to, order);
                    break;
                default:
                    throw TypeDBException.of(UNRECOGNISED_VALUE);
            }
        } else if (structureEdge.isEqual()) {
            return new ProcedureEdge.Equal(from, to, order, Encoding.Direction.Edge.FORWARD);
        } else throw TypeDBException.of(ILLEGAL_STATE);
        registerEdge(edge);
        return edge;
    }

    private ProcedureEdge<?, ?> createIn(ProcedureVertex.Type from, ProcedureVertex.Type to, StructureEdge<?, ?> structureEdge,
                                         int order) {
        ProcedureEdge<?, ?> edge;
        if (structureEdge.isNative()) {
            switch (structureEdge.asNative().encoding().asType()) {
                case SUB:
                    edge = new ProcedureEdge.Native.Type.Sub.Backward(from, to, order, structureEdge.asNative().isTransitive());
                    break;
                case OWNS:
                    edge = new ProcedureEdge.Native.Type.Owns.Backward(from, to, order, false);
                    break;
                case OWNS_KEY:
                    edge = new ProcedureEdge.Native.Type.Owns.Backward(from, to, order, true);
                    break;
                case PLAYS:
                    edge = new ProcedureEdge.Native.Type.Plays.Backward(from, to, order);
                    break;
                case RELATES:
                    edge = new ProcedureEdge.Native.Type.Relates.Backward(from, to, order);
                    break;
                default:
                    throw TypeDBException.of(UNRECOGNISED_VALUE);
            }
        } else if (structureEdge.isEqual()) {
            return new ProcedureEdge.Equal(from, to, order, Encoding.Direction.Edge.BACKWARD);
        } else throw TypeDBException.of(ILLEGAL_STATE);
        registerEdge(edge);
        return edge;
    }

    private void registerEdge(ProcedureEdge<?, ?> edge) {
        edge.from().out(edge);
        edge.to().in(edge);
    }
}
