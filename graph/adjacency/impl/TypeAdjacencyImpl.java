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
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.graph.adjacency.TypeAdjacency;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.Storage;
import com.vaticle.typedb.core.graph.edge.Edge;
import com.vaticle.typedb.core.graph.edge.TypeEdge;
import com.vaticle.typedb.core.graph.edge.impl.TypeEdgeImpl;
import com.vaticle.typedb.core.graph.iid.EdgeIID;
import com.vaticle.typedb.core.graph.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.collection.ByteArray.join;
import static com.vaticle.typedb.core.common.iterator.Iterators.empty;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;

public abstract class TypeAdjacencyImpl implements TypeAdjacency {

    final TypeVertex owner;
    final Encoding.Direction.Adjacency direction;
    final ConcurrentMap<Encoding.Edge.Type, ConcurrentSet<TypeEdge>> edges;

    TypeAdjacencyImpl(TypeVertex owner, Encoding.Direction.Adjacency direction) {
        this.owner = owner;
        this.direction = direction;
        this.edges = new ConcurrentHashMap<>();
    }

    private void putNonRecursive(TypeEdge edge) {
        assert !owner.isDeleted();
        cache(edge);
        owner.setModified();
    }

    @Override
    public TypeEdge put(Encoding.Edge.Type encoding, TypeVertex adjacent) {
        assert !owner.isDeleted();
        TypeVertex from = direction.isOut() ? owner : adjacent;
        TypeVertex to = direction.isOut() ? adjacent : owner;
        TypeEdgeImpl edge = new TypeEdgeImpl.Buffered(encoding, from, to);
        edges.computeIfAbsent(encoding, e -> new ConcurrentSet<>()).add(edge);
        if (direction.isOut()) ((TypeAdjacencyImpl) to.ins()).putNonRecursive(edge);
        else ((TypeAdjacencyImpl) from.outs()).putNonRecursive(edge);
        owner.setModified();
        return edge;
    }

    @Override
    public TypeEdge cache(TypeEdge edge) {
        edges.computeIfAbsent(edge.encoding(), e -> new ConcurrentSet<>()).add(edge);
        return edge;
    }

    @Override
    public void remove(TypeEdge edge) {
        if (edges.containsKey(edge.encoding())) {
            edges.get(edge.encoding()).remove(edge);
            owner.setModified();
        }
    }

    @Override
    public void deleteAll() {
        for (Encoding.Edge.Type encoding : Encoding.Edge.Type.values()) delete(encoding);
    }

    @Override
    public void commit() {
        edges.values().forEach(set -> set.forEach(Edge::commit));
    }

    public static class Buffered extends TypeAdjacencyImpl implements TypeAdjacency {

        public Buffered(TypeVertex owner, Encoding.Direction.Adjacency direction) {
            super(owner, direction);
        }

        @Override
        public TypeIteratorBuilder edge(Encoding.Edge.Type encoding) {
            ConcurrentSet<TypeEdge> t = edges.get(encoding);
            if (t != null) return new TypeIteratorBuilder(iterate(t.iterator()));
            return new TypeIteratorBuilder(empty());
        }

        @Override
        public TypeEdge edge(Encoding.Edge.Type encoding, TypeVertex adjacent) {
            if (edges.containsKey(encoding)) {
                Predicate<TypeEdge> predicate = direction.isOut()
                        ? e -> e.to().equals(adjacent)
                        : e -> e.from().equals(adjacent);
                return edges.get(encoding).stream().filter(predicate).findAny().orElse(null);
            }
            return null;
        }

        @Override
        public void delete(Encoding.Edge.Type encoding) {
            if (edges.containsKey(encoding)) edges.get(encoding).forEach(Edge::delete);
        }
    }

    public static class Persisted extends TypeAdjacencyImpl implements TypeAdjacency {

        private final ConcurrentSet<Encoding.Edge.Type> fetched;
        private final boolean isReadOnly;

        public Persisted(TypeVertex owner, Encoding.Direction.Adjacency direction) {
            super(owner, direction);
            fetched = new ConcurrentSet<>();
            isReadOnly = owner.graph().isReadOnly();
        }

        private ByteArray edgeIID(Encoding.Edge.Type encoding, TypeVertex adjacent) {
            return join(owner.iid().bytes(),
                        direction.isOut() ? encoding.out().bytes() : encoding.in().bytes(),
                        adjacent.iid().bytes());
        }

        private TypeEdge newPersistedEdge(ByteArray key, ByteArray value) {
            VertexIID.Type overridden = ((value.length() == 0) ? null : VertexIID.Type.of(value));
            return new TypeEdgeImpl.Persisted(owner.graph(), EdgeIID.Type.of(key), overridden);
        }

        private FunctionalIterator<TypeEdge> edgeIterator(Encoding.Edge.Type encoding) {
            ConcurrentSet<TypeEdge> bufferedEdges;
            if (isReadOnly && fetched.contains(encoding)) {
                return (bufferedEdges = edges.get(encoding)) != null ? iterate(bufferedEdges) : empty();
            }

            ByteArray iid = join(owner.iid().bytes(), direction.isOut() ? encoding.out().bytes() : encoding.in().bytes());
            FunctionalIterator<TypeEdge> storageIterator = owner.graph().storage()
                    .iterate(iid, Storage.SortedPair::new).map(pair -> cache(newPersistedEdge(pair.first(), pair.second())));
            if (isReadOnly) storageIterator = storageIterator.onConsumed(() -> fetched.add(encoding));
            if ((bufferedEdges = edges.get(encoding)) == null) return storageIterator;
            else return link(bufferedEdges.iterator(), storageIterator).distinct();
        }

        @Override
        public TypeIteratorBuilder edge(Encoding.Edge.Type encoding) {
            return new TypeIteratorBuilder(edgeIterator(encoding));
        }

        @Override
        public TypeEdge edge(Encoding.Edge.Type encoding, TypeVertex adjacent) {
            Optional<TypeEdge> container;
            Predicate<TypeEdge> predicate = direction.isOut()
                    ? e -> e.to().equals(adjacent)
                    : e -> e.from().equals(adjacent);

            if (edges.containsKey(encoding) &&
                    (container = edges.get(encoding).stream().filter(predicate).findAny()).isPresent()) {
                return container.get();
            } else {
                ByteArray edgeIID = edgeIID(encoding, adjacent);
                ByteArray overriddenIID;
                if ((overriddenIID = owner.graph().storage().get(edgeIID)) != null) {
                    return cache(newPersistedEdge(edgeIID, overriddenIID));
                }
            }

            return null;
        }

        @Override
        public void delete(Encoding.Edge.Type encoding) {
            edgeIterator(encoding).forEachRemaining(Edge::delete);
        }

        @Override
        public void deleteAll() {
            for (Encoding.Edge.Type type : Encoding.Edge.Type.values()) delete(type);
        }
    }
}
