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

package hypergraph.graph.adjacency;

import hypergraph.common.iterator.Iterators;
import hypergraph.graph.adjacency.impl.AdjacencyImpl;
import hypergraph.graph.edge.Edge;
import hypergraph.graph.edge.TypeEdge;
import hypergraph.graph.edge.impl.TypeEdgeImpl;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.TypeVertex;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static hypergraph.common.collection.ByteArrays.join;
import static hypergraph.common.iterator.Iterators.link;

public abstract class TypeAdjacencyImpl extends AdjacencyImpl<Schema.Edge.Type, TypeEdge, TypeVertex> implements TypeAdjacency {

    TypeAdjacencyImpl(TypeVertex owner, Direction direction) {
        super(owner, direction);
    }

    @Override
    public void put(Schema.Edge.Type schema, TypeVertex adjacent) {
        TypeVertex from = direction.isOut() ? owner : adjacent;
        TypeVertex to = direction.isOut() ? adjacent : owner;
        TypeEdge edge = new TypeEdgeImpl.Buffered(owner.graph(), schema, from, to);
        edges.computeIfAbsent(edge.schema(), e -> ConcurrentHashMap.newKeySet()).add(edge);
        to.ins().putNonRecursive(edge);
    }

    @Override
    public void deleteNonRecursive(TypeEdge edge) {
        if (edges.containsKey(edge.schema())) edges.get(edge.schema()).remove(edge);
    }

    @Override
    public void deleteAll() {
        for (Schema.Edge.Type schema : Schema.Edge.Type.values()) delete(schema);
    }

    public static class TypeIteratorBuilderImpl
            extends IteratorBuilderImpl<TypeVertex, TypeEdge>
            implements TypeAdjacency.TypeIteratorBuilder {

        TypeIteratorBuilderImpl(Iterator<TypeEdge> edgeIterator) {
            super(edgeIterator);
        }

        public Iterator<TypeVertex> overridden() {
            return Iterators.apply(edgeIterator, TypeEdge::overridden);
        }
    }

    public static class Buffered extends TypeAdjacencyImpl {

        public Buffered(TypeVertex owner, Direction direction) {
            super(owner, direction);
        }

        @Override
        public TypeIteratorBuilderImpl edge(Schema.Edge.Type schema) {
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
        public void delete(Schema.Edge.Type schema, TypeVertex adjacent) {
            if (edges.containsKey(schema)) {
                Predicate<TypeEdge> predicate = direction.isOut()
                        ? e -> e.to().equals(adjacent)
                        : e -> e.from().equals(adjacent);
                edges.get(schema).stream().filter(predicate).forEach(Edge::delete);
            }
        }

        @Override
        public void delete(Schema.Edge.Type schema) {
            if (edges.containsKey(schema)) edges.get(schema).forEach(Edge::delete);
        }
    }

    public static class Persisted extends TypeAdjacencyImpl {

        public Persisted(TypeVertex owner, Direction direction) {
            super(owner, direction);
        }

        @Override
        public TypeIteratorBuilderImpl edge(Schema.Edge.Type schema) {
            Iterator<TypeEdge> storageIterator = owner.graph().storage().iterate(
                    join(owner.iid(), direction.isOut() ? schema.out().key() : schema.in().key()),
                    (key, value) -> new TypeEdgeImpl.Persisted(owner.graph(), key, value)
            );

            if (edges.get(schema) == null) {
                return new TypeIteratorBuilderImpl(storageIterator);
            } else {
                return new TypeIteratorBuilderImpl(link(edges.get(schema).iterator(), storageIterator));
            }
        }

        @Override
        public TypeEdge edge(Schema.Edge.Type schema, TypeVertex adjacent) {
            Optional<TypeEdge> container;
            Predicate<TypeEdge> predicate = direction.isOut()
                    ? e -> e.to().equals(adjacent)
                    : e -> e.from().equals(adjacent);

            if (edges.containsKey(schema) &&
                    (container = edges.get(schema).stream().filter(predicate).findAny()).isPresent()) {
                return container.get();
            } else {
                Schema.Infix infix = direction.isOut() ? schema.out() : schema.in();
                byte[] edgeIID = join(owner.iid(), infix.key(), adjacent.iid());
                byte[] overriddenIID;
                if ((overriddenIID = owner.graph().storage().get(edgeIID)) != null) {
                    return new TypeEdgeImpl.Persisted(owner.graph(), edgeIID, overriddenIID);
                }
            }

            return null;
        }

        @Override
        public void delete(Schema.Edge.Type schema, TypeVertex adjacent) {
            Optional<TypeEdge> container;
            Predicate<TypeEdge> predicate = direction.isOut()
                    ? e -> e.to().equals(adjacent)
                    : e -> e.from().equals(adjacent);

            if (edges.containsKey(schema) &&
                    (container = edges.get(schema).stream().filter(predicate).findAny()).isPresent()) {
                edges.get(schema).remove(container.get());
            } else {
                Schema.Infix infix = direction.isOut() ? schema.out() : schema.in();
                byte[] edgeIID = join(owner.iid(), infix.key(), adjacent.iid());
                byte[] overriddenIID;
                if ((overriddenIID = owner.graph().storage().get(edgeIID)) != null) {
                    (new TypeEdgeImpl.Persisted(owner.graph(), edgeIID, overriddenIID)).delete();
                }
            }
        }

        @Override
        public void delete(Schema.Edge.Type schema) {
            if (edges.containsKey(schema)) edges.get(schema).parallelStream().forEach(Edge::delete);
            Iterator<TypeEdge> storageIterator = owner.graph().storage().iterate(
                    join(owner.iid(), direction.isOut() ? schema.out().key() : schema.in().key()),
                    (key, value) -> new TypeEdgeImpl.Persisted(owner.graph(), key, value)
            );
            storageIterator.forEachRemaining(Edge::delete);
        }
    }
}
