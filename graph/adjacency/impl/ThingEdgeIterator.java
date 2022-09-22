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

package com.vaticle.typedb.core.graph.adjacency.impl;

import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.graph.adjacency.ThingAdjacency;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.graph.edge.impl.ThingEdgeImpl;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.iterateSorted;

public abstract class ThingEdgeIterator {

    static class InEdgeIteratorImpl implements ThingAdjacency.In.InEdgeIterator {

        private final ThingVertex owner;
        private final Forwardable<ThingEdge.View.Backward, Order.Asc> edges;
        private final Encoding.Edge.Thing encoding;
        private final TypeVertex optimisedType;

        InEdgeIteratorImpl(Forwardable<ThingEdge.View.Backward, Order.Asc> edges, ThingVertex owner,
                           Encoding.Edge.Thing encoding) {
            this(edges, owner, encoding, null);
        }

        InEdgeIteratorImpl(Forwardable<ThingEdge.View.Backward, Order.Asc> edges, ThingVertex owner,
                           Encoding.Edge.Thing encoding, @Nullable TypeVertex optimisedType) {
            this.owner = owner;
            this.edges = edges;
            this.encoding = encoding;
            this.optimisedType = optimisedType;
        }

        @Override
        public Forwardable<ThingVertex, Order.Asc> from() {
            return edges.mapSorted(view -> view.edge().from(), this::targetEdge, ASC);
        }

        @Override
        public SortedIterator<ThingVertex, Order.Asc> to() {
            return iterateSorted(ASC, list(owner));
        }

        @Override
        public Forwardable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> fromAndOptimised() {
            assert encoding.isOptimisation();
            return edges.mapSorted(
                    edgeView -> KeyValue.of(edgeView.edge().from(), edgeView.edge().optimised().get()),
                    fromAndOptimised -> targetEdge(fromAndOptimised.key()),
                    ASC
            );
        }

        ThingEdge.View.Backward targetEdge(ThingVertex targetFrom) {
            return new ThingEdgeImpl.Target(encoding, targetFrom, owner, optimisedType).backwardView();
        }
    }

    static class OutEdgeIteratorImpl implements ThingAdjacency.Out.OutEdgeIterator {

        private final ThingVertex owner;
        private final Forwardable<ThingEdge.View.Forward, Order.Asc> edges;
        private final Encoding.Edge.Thing encoding;
        private final TypeVertex optimisedType;

        OutEdgeIteratorImpl(Forwardable<ThingEdge.View.Forward, Order.Asc> edges, ThingVertex owner,
                            Encoding.Edge.Thing encoding) {
            this(edges, owner, encoding, null);
        }

        OutEdgeIteratorImpl(Forwardable<ThingEdge.View.Forward, Order.Asc> edges, ThingVertex owner,
                            Encoding.Edge.Thing encoding, @Nullable TypeVertex optimisedType) {
            this.owner = owner;
            this.edges = edges;
            this.encoding = encoding;
            this.optimisedType = optimisedType;
        }

        @Override
        public SortedIterator<ThingVertex, Order.Asc> from() {
            return iterateSorted(ASC, list(owner));
        }

        @Override
        public Forwardable<ThingVertex, Order.Asc> to() {
            return edges.mapSorted(view -> view.edge().to(), this::targetEdge, ASC);
        }

        @Override
        public Forwardable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> toAndOptimised() {
            assert encoding.isOptimisation();
            return edges.mapSorted(
                    edgeView -> KeyValue.of(edgeView.edge().to(), edgeView.edge().optimised().get()),
                    toAndOptimised -> targetEdge(toAndOptimised.key()),
                    ASC
            );
        }

        ThingEdge.View.Forward targetEdge(ThingVertex targetTo) {
            return new ThingEdgeImpl.Target(encoding, owner, targetTo, optimisedType).forwardView();
        }
    }
}
