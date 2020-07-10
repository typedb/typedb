/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graph.adjacency.impl;

import grakn.core.graph.adjacency.Adjacency;
import grakn.core.graph.adjacency.TypeAdjacency;
import grakn.core.graph.edge.Edge;
import grakn.core.graph.edge.TypeEdge;
import grakn.core.graph.edge.impl.TypeEdgeImpl;
import grakn.core.graph.iid.EdgeIID;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Schema;
import grakn.core.graph.vertex.TypeVertex;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static grakn.core.common.collection.Bytes.join;
import static grakn.core.common.iterator.Iterators.apply;
import static grakn.core.common.iterator.Iterators.distinct;
import static grakn.core.common.iterator.Iterators.link;

public abstract class TypeAdjacencyImpl implements TypeAdjacency {

    final TypeVertex owner;
    final Adjacency.Direction direction;
    final ConcurrentMap<Schema.Edge.Type, Set<TypeEdge>> edges;

    TypeAdjacencyImpl(TypeVertex owner, Adjacency.Direction direction) {
        this.owner = owner;
        this.direction = direction;
        this.edges = new ConcurrentHashMap<>();
    }

    private void putNonRecursive(TypeEdge edge) {
        loadToBuffer(edge);
        owner.setModified();
    }

    @Override
    public TypeEdgeImpl put(Schema.Edge.Type schema, TypeVertex adjacent) {
        TypeVertex from = direction.isOut() ? owner : adjacent;
        TypeVertex to = direction.isOut() ? adjacent : owner;
        TypeEdgeImpl edge = new TypeEdgeImpl.Buffered(schema, from, to);
        edges.computeIfAbsent(schema, e -> ConcurrentHashMap.newKeySet()).add(edge);
        if (direction.isOut()) ((TypeAdjacencyImpl) to.ins()).putNonRecursive(edge);
        else ((TypeAdjacencyImpl) from.outs()).putNonRecursive(edge);
        owner.setModified();
        return edge;
    }

    @Override
    public void loadToBuffer(TypeEdge edge) {
        edges.computeIfAbsent(edge.schema(), e -> ConcurrentHashMap.newKeySet()).add(edge);
    }

    @Override
    public void removeFromBuffer(TypeEdge edge) {
        if (edges.containsKey(edge.schema())) {
            edges.get(edge.schema()).remove(edge);
            owner.setModified();
        }
    }

    @Override
    public void deleteAll() {
        for (Schema.Edge.Type schema : Schema.Edge.Type.values()) delete(schema);
    }

    static class TypeIteratorBuilderImpl implements TypeAdjacency.TypeIteratorBuilder {

        private final Iterator<TypeEdge> edgeIterator;

        TypeIteratorBuilderImpl(Iterator<TypeEdge> edgeIterator) {
            this.edgeIterator = edgeIterator;
        }

        @Override
        public Iterator<TypeVertex> to() {
            return apply(edgeIterator, Edge::to);
        }

        @Override
        public Iterator<TypeVertex> from() {
            return apply(edgeIterator, Edge::from);
        }

        public Iterator<TypeVertex> overridden() {
            return apply(edgeIterator, TypeEdge::overridden);
        }
    }

    public static class Buffered extends TypeAdjacencyImpl implements TypeAdjacency {

        public Buffered(TypeVertex owner, Direction direction) {
            super(owner, direction);
        }

        @Override
        public TypeIteratorBuilder edge(Schema.Edge.Type schema) {
            Set<TypeEdge> t;
            if ((t = edges.get(schema)) != null) return new TypeIteratorBuilderImpl(t.iterator());
            return new TypeIteratorBuilderImpl(Collections.emptyIterator());
        }

        @Override
        public TypeEdge edge(Schema.Edge.Type schema, TypeVertex adjacent) {
            if (edges.containsKey(schema)) {
                Predicate<TypeEdge> predicate = direction.isOut()
                        ? e -> e.to().equals(adjacent)
                        : e -> e.from().equals(adjacent);
                return edges.get(schema).stream().filter(predicate).findAny().orElse(null);
            }
            return null;
        }

        @Override
        public void delete(Schema.Edge.Type schema) {
            if (edges.containsKey(schema)) edges.get(schema).forEach(Edge::delete);
        }

        @Override
        public void forEach(Consumer<TypeEdge> function) {
            edges.forEach((key, set) -> set.forEach(function));
        }
    }

    public static class Persisted extends TypeAdjacencyImpl implements TypeAdjacency {

        public Persisted(TypeVertex owner, Direction direction) {
            super(owner, direction);
        }

        private byte[] edgeIID(Schema.Edge.Type schema, TypeVertex adjacent) {
            return join(owner.iid().bytes(),
                        direction.isOut() ? schema.out().bytes() : schema.in().bytes(),
                        adjacent.iid().bytes());
        }

        private TypeEdge newPersistedEdge(byte[] key, byte[] value) {
            VertexIID.Type overridden = ((value.length == 0) ? null : VertexIID.Type.of(value));
            return new TypeEdgeImpl.Persisted(owner.graph(), EdgeIID.Type.of(key), overridden);
        }

        private Iterator<TypeEdge> edgeIterator(Schema.Edge.Type schema) {
            Iterator<TypeEdge> storageIterator = owner.graph().storage().iterate(
                    join(owner.iid().bytes(), direction.isOut() ? schema.out().bytes() : schema.in().bytes()),
                    this::newPersistedEdge
            );

            if (edges.get(schema) == null) {
                return storageIterator;
            } else {
                return distinct(link(edges.get(schema).iterator(), storageIterator));
            }
        }

        @Override
        public TypeIteratorBuilder edge(Schema.Edge.Type schema) {
            return new TypeIteratorBuilderImpl(edgeIterator(schema));
        }

        @Override
        public TypeEdge edge(Schema.Edge.Type schema, TypeVertex adjacent) {
            Optional<TypeEdge> container;
            Predicate<TypeEdge> predicate = direction.isOut()
                    ? e -> e.to().equals(adjacent)
                    : e -> e.from().equals(adjacent);

            if (edges.containsKey(schema) && (container = edges.get(schema).stream().filter(predicate).findAny()).isPresent()) {
                return container.get();
            } else {
                byte[] edgeIID = edgeIID(schema, adjacent);
                byte[] overriddenIID;
                if ((overriddenIID = owner.graph().storage().get(edgeIID)) != null) {
                    return newPersistedEdge(edgeIID, overriddenIID);
                }
            }

            return null;
        }

        @Override
        public void delete(Schema.Edge.Type schema) {
            if (edges.containsKey(schema)) edges.get(schema).parallelStream().forEach(Edge::delete);
            Iterator<TypeEdge> storageIterator = owner.graph().storage().iterate(
                    join(owner.iid().bytes(), direction.isOut() ? schema.out().bytes() : schema.in().bytes()),
                    this::newPersistedEdge
            );
            storageIterator.forEachRemaining(Edge::delete);
        }

        @Override
        public void forEach(Consumer<TypeEdge> function) {
            for (Schema.Edge.Type schema : Schema.Edge.Type.values()) {
                edgeIterator(schema).forEachRemaining(function);
            }
        }
    }
}
