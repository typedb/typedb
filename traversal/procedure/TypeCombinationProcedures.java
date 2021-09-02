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
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.traversal.TypeTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.graph.TraversalVertex;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class TypeCombinationProcedures {

    private final TypeTraversal traversal;
    private final Identifier[] vertexOrder;

    TypeCombinationProcedures(TypeTraversal traversal) {
        this.traversal = traversal;
        this.vertexOrder = vertexOrder(traversal);
    }

    public static TypeCombinationProcedures of(TypeTraversal traversal) {
        return new TypeCombinationProcedures(traversal);
    }

    private static Identifier[] vertexOrder(TypeTraversal traversal) {
        return traversal.structure().vertices().stream().filter(
                vertex -> !vertex.id().isLabel() && traversal.filter().contains(vertex.id().asVariable().asRetrievable())
        ).sorted(Comparator.comparing(vertex -> {
            int labels = vertex.asType().props().labels().size();
            return labels > 0 ? labels : Integer.MAX_VALUE;
        })).map(TraversalVertex::id).toArray(Identifier[]::new);
    }

    public VertexCombinationProcedure first() {
        return new VertexCombinationProcedure(0, new HashMap<>());
    }

    public VertexCombinationProcedure next(VertexCombinationProcedure previous, Set<TypeVertex> previousCombination) {
        assert !previous.vertexTypes.containsKey(previous.vertexId()) && previous.position + 1 < vertexOrder.length;
        Map<Identifier, Set<Label>> evaluatedTypes = new HashMap<>(previous.vertexTypes);
        evaluatedTypes.put(previous.vertexId(), iterate(previousCombination).map(TypeVertex::properLabel).toSet());
        return new VertexCombinationProcedure(previous.position + 1, evaluatedTypes);
    }

    public int proceduresCount() {
        return vertexOrder.length;
    }

    public class VertexCombinationProcedure {

        private final int position;
        private final Map<Identifier, Set<Label>> vertexTypes;

        private VertexCombinationProcedure(int position, Map<Identifier, Set<Label>> vertexTypes) {
            this.position = position;
            this.vertexTypes = vertexTypes;
        }

        public Identifier vertexId() {
            return vertexOrder[position];
        }

        public GraphProcedure permutationProcedure() {
            GraphProcedure.Builder builder = GraphProcedure.builder();
            Set<StructureEdge<?, ?>> visitedEdges = new HashSet<>();
            StructureVertex.Type start = traversal.structure().typeVertex(vertexOrder[position]);
            visitDfs(start, visitedEdges, builder);
            return builder.build();
        }

        private ProcedureVertex.Type visitDfs(StructureVertex.Type structureVertex, Set<StructureEdge<?, ?>> visitedEdges,
                                              GraphProcedure.Builder builder) {
            boolean isStart = builder.vertices().size() == 0;
            if (builder.containsVertex(structureVertex.id())) return builder.getType(structureVertex.id());
            ProcedureVertex.Type procedureVertex = createVertex(structureVertex, isStart, builder);
            visitOut(procedureVertex, structureVertex, builder, visitedEdges);
            visitIn(procedureVertex, structureVertex, builder, visitedEdges);
            return procedureVertex;
        }

        private ProcedureVertex.Type createVertex(StructureVertex.Type structureVertex, boolean isStart,
                                                  GraphProcedure.Builder builder) {
            ProcedureVertex.Type vertex = builder.type(structureVertex.id(), isStart);
            vertex.props(new TraversalVertex.Properties.Type(structureVertex.props()));
            if (vertexTypes.containsKey(vertex.id())) {
                vertex.props().clearLabels();
                vertex.props().labels(vertexTypes.get(vertex.id()));
            };
            return vertex;
        }

        private void visitOut(ProcedureVertex.Type procedureVertex, StructureVertex.Type structureVertex,
                              GraphProcedure.Builder builder, Set<StructureEdge<?, ?>> visitedEdges) {
            structureVertex.outs().forEach(structureEdge -> {
                boolean toStart = builder.containsVertex(structureEdge.to().id())
                        && builder.getType(structureEdge.to().id()).isStartingVertex();
                if (!visitedEdges.contains(structureEdge) && (!toStart || procedureVertex.isStartingVertex())) {
                    visitedEdges.add(structureEdge);
                    int order = visitedEdges.size();
                    ProcedureVertex.Type end = visitDfs(structureEdge.to().asType(), visitedEdges, builder);
                    createOut(procedureVertex, end, structureEdge, order, builder);
                }
            });
        }

        private void visitIn(ProcedureVertex.Type procedureVertex, StructureVertex.Type structureVertex,
                             GraphProcedure.Builder builder, Set<StructureEdge<?, ?>> visitedEdges) {
            structureVertex.ins().forEach(structureEdge -> {
                boolean fromStart = builder.containsVertex(structureEdge.from().id())
                        && builder.getType(structureEdge.from().id()).isStartingVertex();
                if (!visitedEdges.contains(structureEdge) && (!fromStart || procedureVertex.isStartingVertex())) {
                    visitedEdges.add(structureEdge);
                    int order = visitedEdges.size();
                    ProcedureVertex.Type start = visitDfs(structureEdge.from().asType(), visitedEdges, builder);
                    createIn(procedureVertex, start, structureEdge, order, builder);
                }
            });
        }

        private void createOut(ProcedureVertex.Type from, ProcedureVertex.Type to, StructureEdge<?, ?> structureEdge,
                               int order, GraphProcedure.Builder builder) {
            if (structureEdge.isNative()) {
                switch (structureEdge.asNative().encoding().asType()) {
                    case SUB:
                        builder.forwardSub(order, from, to, structureEdge.asNative().isTransitive());
                        break;
                    case OWNS:
                        builder.forwardOwns(order, from, to, false);
                        break;
                    case OWNS_KEY:
                        builder.forwardOwns(order, from, to, true);
                        break;
                    case PLAYS:
                        builder.forwardPlays(order, from, to);
                        break;
                    case RELATES:
                        builder.forwardRelates(order, from, to);
                        break;
                    default:
                        throw TypeDBException.of(UNRECOGNISED_VALUE);
                }
            } else if (structureEdge.isEqual()) {
                builder.forwardEqual(order, from, to);
            } else throw TypeDBException.of(ILLEGAL_STATE);
        }

        private void createIn(ProcedureVertex.Type from, ProcedureVertex.Type to, StructureEdge<?, ?> structureEdge, int order, GraphProcedure.Builder builder) {
            if (structureEdge.isNative()) {
                switch (structureEdge.asNative().encoding().asType()) {
                    case SUB:
                        builder.backwardSub(order, from, to, structureEdge.asNative().isTransitive());
                        break;
                    case OWNS:
                        builder.backwardOwns(order, from, to, false);
                        break;
                    case OWNS_KEY:
                        builder.backwardOwns(order, from, to, true);
                        break;
                    case PLAYS:
                        builder.backwardPlays(order, from, to);
                        break;
                    case RELATES:
                        builder.backwardRelates(order, from, to);
                        break;
                    default:
                        throw TypeDBException.of(UNRECOGNISED_VALUE);
                }
            } else if (structureEdge.isEqual()) {
                builder.backwardEqual(order, from, to);
            } else throw TypeDBException.of(ILLEGAL_STATE);
        }
    }


}
