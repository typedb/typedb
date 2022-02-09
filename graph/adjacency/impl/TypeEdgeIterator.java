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
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Seekable;
import com.vaticle.typedb.core.graph.adjacency.TypeAdjacency;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.edge.TypeEdge;
import com.vaticle.typedb.core.graph.edge.impl.TypeEdgeImpl;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.iterator.Iterators.Sorted.iterateSorted;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;

public abstract class TypeEdgeIterator {

    static class InEdgeIteratorImpl implements TypeAdjacency.In.InEdgeIterator {

        final TypeVertex owner;
        final Seekable<TypeEdge.View.Backward, Order.Asc> edges;
        final Encoding.Edge.Type encoding;

        InEdgeIteratorImpl(Seekable<TypeEdge.View.Backward, Order.Asc> edges, TypeVertex owner, Encoding.Edge.Type encoding) {
            this.owner = owner;
            this.edges = edges;
            this.encoding = encoding;
        }

        @Override
        public Seekable<TypeVertex, Order.Asc> from() {
            return edges.mapSorted(edgeView -> edgeView.edge().from(), this::targetEdge, ASC);
        }

        @Override
        public SortedIterator<TypeVertex, Order.Asc> to() {
            return iterateSorted(ASC, list(owner));
        }

        @Override
        public FunctionalIterator<TypeVertex> overridden() {
            return edges.map(edgeView -> edgeView.edge().overridden());
        }

        @Override
        public Seekable<KeyValue<TypeVertex, TypeVertex>, Order.Asc> fromAndOverridden() {
            return edges.mapSorted(
                    edgeView -> KeyValue.of(edgeView.edge().from(), edgeView.edge().overridden()),
                    fromAndOverridden -> targetEdge(fromAndOverridden.key()),
                    ASC
            );
        }

        TypeEdge.View.Backward targetEdge(TypeVertex targetFrom) {
            return new TypeEdgeImpl.Target(encoding, targetFrom, owner).getBackward();
        }
    }

    static class OutEdgeIteratorImpl implements TypeAdjacency.Out.OutEdgeIterator {

        final TypeVertex owner;
        final Seekable<TypeEdge.View.Forward, Order.Asc> edges;
        final Encoding.Edge.Type encoding;

        OutEdgeIteratorImpl(Seekable<TypeEdge.View.Forward, Order.Asc> edges, TypeVertex owner, Encoding.Edge.Type encoding) {
            this.owner = owner;
            this.edges = edges;
            this.encoding = encoding;
        }

        @Override
        public SortedIterator<TypeVertex, Order.Asc> from() {
            return iterateSorted(ASC, list(owner));
        }

        @Override
        public Seekable<TypeVertex, Order.Asc> to() {
            return edges.mapSorted(edgeView -> edgeView.edge().to(), this::targetEdge, ASC);
        }

        @Override
        public FunctionalIterator<TypeVertex> overridden() {
            return edges.map(edgeView -> edgeView.edge().overridden());
        }

        @Override
        public Seekable<KeyValue<TypeVertex, TypeVertex>, Order.Asc> toAndOverridden() {
            return edges.mapSorted(
                    edgeView -> KeyValue.of(edgeView.edge().to(), edgeView.edge().overridden()),
                    toAndOverridden -> targetEdge(toAndOverridden.key()),
                    ASC
            );
        }

        TypeEdge.View.Forward targetEdge(TypeVertex targetTo) {
            return new TypeEdgeImpl.Target(encoding, owner, targetTo).getForward();
        }
    }
}
