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
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
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

    private final Map<Identifier, ReachableGraph> graphs;

    CombinationProcedure(Map<Identifier, ReachableGraph> graphs) {
        this.graphs = graphs;
    }

    public static CombinationProcedure create(FunctionalIterator<Structure> structures) {
        Map<Identifier, ReachableGraph> graphs = new HashMap<>();
        structures.forEachRemaining(structure -> {
            StructureVertex.Type startVertex = structure.vertices().iterator().next().asType();
            ReachableGraph reachableGraph = new ReachableGraph();
            Set<StructureEdge<?, ?>> visitedEdges = new HashSet<>();
            reachableGraph.registerBFS(startVertex, visitedEdges, true, reachableGraph);
            graphs.put(startVertex.id(), reachableGraph);
        });
        return new CombinationProcedure(graphs);
    }

    public Set<Identifier> startIds() {
        return graphs.keySet();
    }

    public ProcedureVertex.Type start(Identifier startId) {
        assert graphs.containsKey(startId);
        return graphs.get(startId).vertices.get(startId);
    }

    public boolean nonTerminal(Identifier startId, ProcedureVertex.Type vertex) {
        assert graphs.containsKey(startId);
        return graphs.get(startId).nonTerminal(vertex);
    }

    public Set<ProcedureVertex.Type> terminals(Identifier startId) {
        assert graphs.containsKey(startId);
        return graphs.get(startId).terminals();
    }

    public Set<ProcedureEdge<?, ?>> forwardEdges(Identifier startId, ProcedureVertex.Type vertex) {
        assert graphs.containsKey(startId);
        return graphs.get(startId).forwardEdges.get(vertex);
    }

    public Set<ProcedureEdge<?, ?>> reverseEdges(Identifier startId, ProcedureVertex.Type vertex) {
        assert graphs.containsKey(startId);
        return graphs.get(startId).reverseEdges.get(vertex);
    }

    private static class ReachableGraph {

        private final Map<Identifier, ProcedureVertex.Type> vertices;
        private final Map<ProcedureVertex.Type, Set<ProcedureEdge<?, ?>>> forwardEdges;
        private final Map<ProcedureVertex.Type, Set<ProcedureEdge<?, ?>>> reverseEdges;

        ReachableGraph() {
            this.vertices = new HashMap<>();
            this.forwardEdges = new HashMap<>();
            this.reverseEdges = new HashMap<>();
        }

        public boolean nonTerminal(ProcedureVertex.Type vertex) {
            return forwardEdges.containsKey(vertex);
        }

        public Set<ProcedureVertex.Type> terminals() {
            Set<ProcedureVertex.Type> terminals = new HashSet<>(reverseEdges.keySet());
            terminals.removeAll(forwardEdges.keySet());
            return terminals;
        }

        private ProcedureVertex.Type vertex(StructureVertex.Type sv, boolean isStart) {
            return vertices.computeIfAbsent(sv.id(), id -> {
                ProcedureVertex.Type vertex = new ProcedureVertex.Type(id, isStart);
                vertex.props(sv.props());
                return vertex;
            });
        }

        private void registerBFS(StructureVertex.Type structureVertex, Set<StructureEdge<?, ?>> visitedEdges,
                                 boolean isStart, ReachableGraph reachableGraph) {
            ProcedureVertex.Type procedureVertex = reachableGraph.vertex(structureVertex, isStart);
            Set<StructureVertex.Type> next = registerOut(procedureVertex, structureVertex, visitedEdges, reachableGraph);
            next.addAll(registerIn(procedureVertex, structureVertex, visitedEdges, reachableGraph));
            next.forEach(vertex -> registerBFS(vertex, visitedEdges, false, reachableGraph));
        }

        private Set<StructureVertex.Type> registerOut(ProcedureVertex.Type procedureVertex, StructureVertex.Type structureVertex,
                                                      Set<StructureEdge<?, ?>> visitedEdges, ReachableGraph reachableGraph) {
            Set<StructureVertex.Type> next = new HashSet<>();
            if (!structureVertex.outs().isEmpty()) {
                structureVertex.outs().forEach(structureEdge -> {
                    if (!visitedEdges.contains(structureEdge)) {
                        visitedEdges.add(structureEdge);
                        int order = visitedEdges.size();
                        ProcedureVertex.Type end = reachableGraph.vertex(structureEdge.to().asType(), false);
                        ProcedureEdge<?, ?> edge = createOut(procedureVertex, end, structureEdge, order);
                        reachableGraph.forwardEdges.computeIfAbsent(procedureVertex, (v) -> new HashSet<>()).add(edge);
                        reachableGraph.reverseEdges.computeIfAbsent(end, (v) -> new HashSet<>()).add(edge.reverse());
                        next.add(structureEdge.to().asType());
                    }
                });
            }
            return next;
        }

        private Set<StructureVertex.Type> registerIn(ProcedureVertex.Type procedureVertex, StructureVertex.Type structureVertex,
                                                     Set<StructureEdge<?, ?>> visitedEdges, ReachableGraph reachableGraph) {
            Set<StructureVertex.Type> next = new HashSet<>();
            if (!structureVertex.ins().isEmpty()) {
                structureVertex.ins().forEach(structureEdge -> {
                    if (!visitedEdges.contains(structureEdge)) {
                        visitedEdges.add(structureEdge);
                        int order = visitedEdges.size();
                        ProcedureVertex.Type start = reachableGraph.vertex(structureEdge.from().asType(), false);
                        ProcedureEdge<?, ?> edge = createIn(procedureVertex, start, structureEdge, order);
                        reachableGraph.forwardEdges.computeIfAbsent(procedureVertex, (v1) -> new HashSet<>()).add(edge);
                        reachableGraph.reverseEdges.computeIfAbsent(start, (v) -> new HashSet<>()).add(edge.reverse());
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

}
