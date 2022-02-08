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

import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Seekable;
import com.vaticle.typedb.core.graph.adjacency.TypeAdjacency;
import com.vaticle.typedb.core.graph.adjacency.impl.TypeEdgeIterator.InEdgeIteratorImpl;
import com.vaticle.typedb.core.graph.adjacency.impl.TypeEdgeIterator.OutEdgeIteratorImpl;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.Storage.Key;
import com.vaticle.typedb.core.graph.edge.TypeEdge;
import com.vaticle.typedb.core.graph.edge.impl.TypeEdgeImpl;
import com.vaticle.typedb.core.graph.iid.EdgeViewIID;
import com.vaticle.typedb.core.graph.iid.InfixIID;
import com.vaticle.typedb.core.graph.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.iterator.Iterators.Sorted.Seekable.emptySorted;
import static com.vaticle.typedb.core.common.iterator.Iterators.Sorted.Seekable.iterateSorted;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;

public abstract class TypeAdjacencyImpl<EDGE_VIEW extends TypeEdge.View<EDGE_VIEW>> implements TypeAdjacency {

    final TypeVertex owner;
    final ConcurrentMap<Encoding.Edge.Type, ConcurrentSkipListSet<EDGE_VIEW>> edges;

    TypeAdjacencyImpl(TypeVertex owner) {
        this.owner = owner;
        this.edges = new ConcurrentHashMap<>();
    }

    abstract EDGE_VIEW getView(TypeEdge edge);

    private void putNonRecursive(TypeEdge edge) {
        assert !owner.isDeleted();
        cache(edge);
        owner.setModified();
    }

    @Override
    public TypeEdge put(Encoding.Edge.Type encoding, TypeVertex adjacent) {
        assert !owner.isDeleted();
        TypeVertex from = isOut() ? owner : adjacent;
        TypeVertex to = isOut() ? adjacent : owner;
        TypeEdgeImpl edge = new TypeEdgeImpl.Buffered(encoding, from, to);
        edges.computeIfAbsent(encoding, e -> new ConcurrentSkipListSet<>()).add(getView(edge));
        if (isOut()) ((TypeAdjacencyImpl<?>) to.ins()).putNonRecursive(edge);
        else ((TypeAdjacencyImpl<?>) from.outs()).putNonRecursive(edge);
        owner.setModified();
        return edge;
    }

    @Override
    public TypeEdge cache(TypeEdge edge) {
        edges.computeIfAbsent(edge.encoding(), e -> new ConcurrentSkipListSet<>()).add(getView(edge));
        return edge;
    }

    @Override
    public void remove(TypeEdge edge) {
        if (edges.containsKey(edge.encoding())) {
            edges.get(edge.encoding()).remove(getView(edge));
            owner.setModified();
        }
    }

    @Override
    public void deleteAll() {
        for (Encoding.Edge.Type encoding : Encoding.Edge.Type.values()) delete(encoding);
    }

    @Override
    public void commit() {
        edges.values().forEach(set -> set.forEach(view -> view.edge().commit()));
    }

    public static abstract class Buffered<EDGE_VIEW extends TypeEdge.View<EDGE_VIEW>>
            extends TypeAdjacencyImpl<EDGE_VIEW> implements TypeAdjacency {

        Buffered(TypeVertex owner) {
            super(owner);
        }

        public static class In extends Buffered<TypeEdge.View.Backward> implements TypeAdjacency.In {

            public In(TypeVertex owner) {
                super(owner);
            }

            @Override
            public InEdgeIterator edge(Encoding.Edge.Type encoding) {
                ConcurrentSkipListSet<TypeEdge.View.Backward> t = edges.get(encoding);
                if (t != null) return new InEdgeIteratorImpl(iterateSorted(ASC, t), owner, encoding);
                return new InEdgeIteratorImpl(emptySorted(), owner, encoding);
            }

            @Override
            public TypeEdge edge(Encoding.Edge.Type encoding, TypeVertex adjacent) {
                if (edges.containsKey(encoding)) {
                    return edges.get(encoding).stream().filter(view -> view.edge().from().equals(adjacent))
                            .findAny().map(TypeEdge.View::edge).orElse(null);
                }
                return null;
            }

            @Override
            TypeEdge.View.Backward getView(TypeEdge edge) {
                return edge.getBackward();
            }
        }

        public static class Out extends Buffered<TypeEdge.View.Forward> implements TypeAdjacency.Out {

            public Out(TypeVertex owner) {
                super(owner);
            }

            @Override
            public OutEdgeIterator edge(Encoding.Edge.Type encoding) {
                ConcurrentSkipListSet<TypeEdge.View.Forward> t = edges.get(encoding);
                if (t != null) return new OutEdgeIteratorImpl(iterateSorted(ASC, t), owner, encoding);
                return new OutEdgeIteratorImpl(emptySorted(), owner, encoding);
            }

            @Override
            TypeEdge.View.Forward getView(TypeEdge edge) {
                return edge.getForward();
            }

            @Override
            public TypeEdge edge(Encoding.Edge.Type encoding, TypeVertex adjacent) {
                if (edges.containsKey(encoding)) {
                    return edges.get(encoding).stream().filter(view -> view.edge().to().equals(adjacent))
                            .findAny().map(TypeEdge.View::edge).orElse(null);
                }
                return null;
            }
        }

        @Override
        public void delete(Encoding.Edge.Type encoding) {
            if (edges.containsKey(encoding)) edges.get(encoding).forEach(view -> view.edge().delete());
        }
    }

    public static abstract class Persisted<EDGE_VIEW extends TypeEdge.View<EDGE_VIEW>>
            extends TypeAdjacencyImpl<EDGE_VIEW> implements TypeAdjacency {

        private final ConcurrentSet<Encoding.Edge.Type> fetched;
        private final boolean isReadOnly;

        Persisted(TypeVertex owner) {
            super(owner);
            fetched = new ConcurrentSet<>();
            isReadOnly = owner.graph().isReadOnly();
        }

        public static class In extends Persisted<TypeEdge.View.Backward> implements TypeAdjacency.In {

            public In(TypeVertex owner) {
                super(owner);
            }

            @Override
            TypeEdge.View.Backward getView(TypeEdge edge) {
                return edge.getBackward();
            }

            @Override
            public InEdgeIterator edge(Encoding.Edge.Type encoding) {
                return new InEdgeIteratorImpl(iterateViews(encoding), owner, encoding);
            }
        }

        public static class Out extends Persisted<TypeEdge.View.Forward> implements TypeAdjacency.Out {

            public Out(TypeVertex owner) {
                super(owner);
            }

            @Override
            TypeEdge.View.Forward getView(TypeEdge edge) {
                return edge.getForward();
            }

            @Override
            public OutEdgeIterator edge(Encoding.Edge.Type encoding) {
                return new OutEdgeIteratorImpl(iterateViews(encoding), owner, encoding);
            }
        }

        private EdgeViewIID.Type edgeIID(Encoding.Edge.Type encoding, TypeVertex adjacent) {
            return EdgeViewIID.Type.of(owner.iid(), isOut() ? encoding.forward() : encoding.backward(), adjacent.iid());
        }

        private TypeEdge newPersistedEdge(EdgeViewIID.Type edge, ByteArray overriddenBytes) {
            VertexIID.Type overridden = ((overriddenBytes.isEmpty()) ? null : VertexIID.Type.of(overriddenBytes));
            return new TypeEdgeImpl.Persisted(owner.graph(), edge, overridden);
        }

        Seekable<EDGE_VIEW, Order.Asc> iterateViews(Encoding.Edge.Type encoding) {
            ConcurrentSkipListSet<EDGE_VIEW> bufferedEdges;
            if (isReadOnly && fetched.contains(encoding)) {
                return (bufferedEdges = edges.get(encoding)) != null ? iterateSorted(ASC, bufferedEdges) : emptySorted();
            }

            Key.Prefix<EdgeViewIID.Type> prefix = EdgeViewIID.Type.prefix(
                    owner.iid(), InfixIID.Type.of(isOut() ? encoding.forward() : encoding.backward())
            );
            Seekable<EDGE_VIEW, Order.Asc> storageIterator = owner.graph().storage().iterate(prefix, ASC)
                    .mapSorted(
                            ASC,
                            kv -> getView(cache(newPersistedEdge(EdgeViewIID.Type.of(kv.key().bytes()), kv.value()))),
                            edgeView -> KeyValue.of(edgeView.iid(), ByteArray.empty())
                    );
            if (isReadOnly) storageIterator = storageIterator.onConsumed(() -> fetched.add(encoding));
            if ((bufferedEdges = edges.get(encoding)) == null) return storageIterator;
            else return iterateSorted(ASC, bufferedEdges).merge(storageIterator).distinct();
        }

        @Override
        public TypeEdge edge(Encoding.Edge.Type encoding, TypeVertex adjacent) {
            Predicate<TypeEdge> predicate = isOut()
                    ? e -> e.to().equals(adjacent)
                    : e -> e.from().equals(adjacent);

            Optional<TypeEdge> container;
            if (edges.containsKey(encoding) && (container = edges.get(encoding).stream().filter(view -> predicate.test(view.edge()))
                    .findAny().map(TypeEdge.View::edge)).isPresent()) {
                return container.get();
            } else {
                EdgeViewIID.Type edgeIID = edgeIID(encoding, adjacent);
                ByteArray overriddenIID;
                if ((overriddenIID = owner.graph().storage().get(edgeIID)) != null) {
                    return cache(newPersistedEdge(edgeIID, overriddenIID));
                }
            }
            return null;
        }

        @Override
        public void delete(Encoding.Edge.Type encoding) {
            iterateViews(encoding).forEachRemaining(view -> view.edge().delete());
        }
    }
}
