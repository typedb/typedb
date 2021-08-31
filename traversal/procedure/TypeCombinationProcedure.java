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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class TypeCombinationProcedure {

    private final TypeTraversal traversal;
    private final Identifier[] vertexOrder;

    TypeCombinationProcedure(TypeTraversal traversal) {
        this.traversal = traversal;
        this.vertexOrder = iterate(traversal.structure().vertices()).filter(vertex -> !vertex.id().isLabel())
                .map(TraversalVertex::id).toList().toArray(new Identifier[0]);
    }

    public static TypeCombinationProcedure of(TypeTraversal traversal) {
        return new TypeCombinationProcedure(traversal);
    }

    public VertexEvaluation begin() {
        return new VertexEvaluation(0, new HashMap<>());
    }

    public int evaluationVertexCount() {
        return vertexOrder.length;
    }

    public int traversalVertexCount() {
        return traversal.structure().vertices().size();
    }

    private int edgeSize() {
        return traversal.structure().edges().size();
    }

    private StructureVertex.Type getVertex(int pos) {
        return traversal.structure().typeVertex(vertexOrder[pos]);
    }

    public class VertexEvaluation {

        private final int position;
        private final Map<Identifier, Set<Label>> evaluatedTypes;

        private VertexEvaluation(int position, Map<Identifier, Set<Label>> evaluatedTypes) {
            this.position = position;
            this.evaluatedTypes = evaluatedTypes;
        }

        public Identifier evaluationId() {
            return vertexOrder[position];
        }

        public VertexEvaluation next(Set<TypeVertex> additionalEvaluatedTypes) {
            assert !evaluatedTypes.containsKey(evaluationId()) && position + 1 < vertexOrder.length;
            Map<Identifier, Set<Label>> evaluatedTypes = new HashMap<>(this.evaluatedTypes);
            evaluatedTypes.put(evaluationId(), iterate(additionalEvaluatedTypes).map(TypeVertex::properLabel).toSet());
            return new VertexEvaluation(position + 1, evaluatedTypes);
        }

        public GraphProcedure procedure() {
            GraphProcedure.Builder builder = GraphProcedure.builder();
            Set<StructureEdge<?, ?>> visitedEdges = new HashSet<>();
            StructureVertex.Type start = getVertex(position);
            registerDFS(start, visitedEdges, builder);
            return builder.build();
        }

        private ProcedureVertex.Type registerDFS(StructureVertex.Type structureVertex, Set<StructureEdge<?, ?>> visitedEdges,
                                                 GraphProcedure.Builder builder) {
            boolean isStart = builder.vertices().size() == 0;
            if (builder.containsVertex(structureVertex.id())) return builder.getType(structureVertex.id());
            ProcedureVertex.Type vertex = builder.type(structureVertex.id(), isStart);
            vertex.props(structureVertex.props());
            if (evaluatedTypes.containsKey(vertex.id())) vertex.props().labels(evaluatedTypes.get(vertex.id()));
            structureVertex.outs().forEach(structureEdge -> {
                boolean toStart = builder.containsVertex(structureEdge.to().id()) && builder.getType(structureEdge.to().id()).isStartingVertex();
                if (!visitedEdges.contains(structureEdge) && (!toStart || isStart)) {
                    visitedEdges.add(structureEdge);
                    int order = visitedEdges.size();
                    ProcedureVertex.Type end = registerDFS(structureEdge.to().asType(), visitedEdges, builder);
                    registerForwards(vertex, end, structureEdge, order, builder);
                }
            });
            structureVertex.ins().forEach(structureEdge -> {
                boolean fromStart = builder.containsVertex(structureEdge.from().id()) && builder.getType(structureEdge.from().id()).isStartingVertex();
                if (!visitedEdges.contains(structureEdge) && (!fromStart || isStart)) {
                    visitedEdges.add(structureEdge);
                    int order = visitedEdges.size();
                    ProcedureVertex.Type start = registerDFS(structureEdge.from().asType(), visitedEdges, builder);
                    registerBackwards(vertex, start, structureEdge, order, builder);
                }
            });
            return vertex;
        }

        private void registerForwards(ProcedureVertex.Type from, ProcedureVertex.Type to, StructureEdge<?, ?> structureEdge,
                                      int order, GraphProcedure.Builder builder) {
            // TODO are these all forward, from -> to?
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

        private void registerBackwards(ProcedureVertex.Type from, ProcedureVertex.Type to, StructureEdge<?, ?> structureEdge, int order, GraphProcedure.Builder builder) {
            // TODO are these all backward, from <- to?
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
