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

package com.vaticle.typedb.core.graph.adjacency.impl;

import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Seekable;
import com.vaticle.typedb.core.graph.adjacency.ThingAdjacency;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.graph.edge.impl.ThingEdgeImpl;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.iterator.Iterators.Sorted.iterateSorted;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;

public abstract class ThingEdgeIterator {

    static class InEdgeIteratorImpl implements ThingAdjacency.In.InEdgeIterator {

        final ThingVertex owner;
        final Seekable<ThingEdge.View.Backward, Order.Asc> edges;
        final Encoding.Edge.Thing encoding;

        InEdgeIteratorImpl(Seekable<ThingEdge.View.Backward, Order.Asc> edges, ThingVertex owner, Encoding.Edge.Thing encoding) {
            this.owner = owner;
            this.edges = edges;
            this.encoding = encoding;
        }

        @Override
        public Seekable<ThingVertex, Order.Asc> from() {
            return edges.mapSorted(view -> view.edge().from(), this::targetEdge, ASC);
        }

        @Override
        public SortedIterator<ThingVertex, Order.Asc> to() {
            return iterateSorted(ASC, list(owner));
        }

        ThingEdge.View.Backward targetEdge(ThingVertex targetFrom) {
            return new ThingEdgeImpl.Target(encoding, targetFrom, owner, null).getBackward();
        }

        public static class Optimised extends InEdgeIteratorImpl implements ThingAdjacency.In.InEdgeIterator.Optimised {

            private final TypeVertex optimisedType;

            public Optimised(Seekable<ThingEdge.View.Backward, Order.Asc> edges, ThingVertex owner, Encoding.Edge.Thing encoding,
                             TypeVertex optimisedType) {
                super(edges, owner, encoding);
                this.optimisedType = optimisedType;
            }

            @Override
            public Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> fromAndOptimised() {
                return edges.mapSorted(
                        edgeView -> KeyValue.of(edgeView.edge().from(), edgeView.edge().optimised().get()),
                        fromAndOptimised -> targetEdge(fromAndOptimised.key()),
                        ASC
                );
            }

            @Override
            ThingEdge.View.Backward targetEdge(ThingVertex targetFrom) {
                return new ThingEdgeImpl.Target(encoding, targetFrom, owner, optimisedType).getBackward();
            }
        }
    }

    static class OutEdgeIteratorImpl implements ThingAdjacency.Out.OutEdgeIterator {

        final ThingVertex owner;
        final Seekable<ThingEdge.View.Forward, Order.Asc> edges;
        final Encoding.Edge.Thing encoding;

        OutEdgeIteratorImpl(Seekable<ThingEdge.View.Forward, Order.Asc> edges, ThingVertex owner, Encoding.Edge.Thing encoding) {
            this.owner = owner;
            this.edges = edges;
            this.encoding = encoding;
        }

        @Override
        public SortedIterator<ThingVertex, Order.Asc> from() {
            return iterateSorted(ASC, list(owner));
        }

        @Override
        public Seekable<ThingVertex, Order.Asc> to() {
            return edges.mapSorted(view -> view.edge().to(), this::targetEdge, ASC);
        }

        ThingEdge.View.Forward targetEdge(ThingVertex targetTo) {
            return new ThingEdgeImpl.Target(encoding, owner, targetTo, null).getForward();
        }

        static class Optimised extends OutEdgeIteratorImpl implements ThingAdjacency.Out.OutEdgeIterator.Optimised {

            private final TypeVertex optimisedType;

            Optimised(Seekable<ThingEdge.View.Forward, Order.Asc> edges, ThingVertex owner,
                      Encoding.Edge.Thing encoding,
                      TypeVertex optimisedType) {
                super(edges, owner, encoding);
                this.optimisedType = optimisedType;
            }

            @Override
            public Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> toAndOptimised() {
                return edges.mapSorted(
                        edgeView -> KeyValue.of(edgeView.edge().to(), edgeView.edge().optimised().get()),
                        toAndOptimised -> targetEdge(toAndOptimised.key()),
                        ASC
                );
            }

            @Override
            ThingEdge.View.Forward targetEdge(ThingVertex targetTo) {
                return new ThingEdgeImpl.Target(encoding, owner, targetTo, optimisedType).getForward();
            }
        }
    }
}
