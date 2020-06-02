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

package hypergraph.graph.adjacency.impl;

import hypergraph.common.iterator.Iterators;
import hypergraph.graph.Graph;
import hypergraph.graph.adjacency.Adjacency;
import hypergraph.graph.edge.Edge;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.Vertex;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static hypergraph.common.collection.Bytes.join;
import static hypergraph.common.iterator.Iterators.link;

public abstract class AdjacencyImpl<
        EDGE_SCHEMA extends Schema.Edge,
        EDGE extends Edge<EDGE_SCHEMA, ?, VERTEX>,
        VERTEX extends Vertex<?, ?, VERTEX, EDGE_SCHEMA, EDGE>,
        ITER_BUILDER extends AdjacencyImpl.IteratorBuilderImpl<EDGE, VERTEX>
        > implements Adjacency<EDGE_SCHEMA, EDGE, VERTEX> {

    protected final VERTEX owner;
    protected final Direction direction;
    protected final ConcurrentMap<EDGE_SCHEMA, Set<EDGE>> edges;

    protected AdjacencyImpl(VERTEX owner, Direction direction) {
        this.owner = owner;
        this.direction = direction;
        this.edges = new ConcurrentHashMap<>();
    }

    protected abstract EDGE_SCHEMA[] schemaValues();

    protected abstract ITER_BUILDER newIteratorBuilder(Iterator<EDGE> edgeIterator);

    protected abstract EDGE newBufferedEdge(EDGE_SCHEMA schema, VERTEX from, VERTEX to);

    @Override
    public void put(EDGE_SCHEMA schema, VERTEX adjacent) {
        VERTEX from = direction.isOut() ? owner : adjacent;
        VERTEX to = direction.isOut() ? adjacent : owner;
        EDGE edge = newBufferedEdge(schema, from, to);
        edges.computeIfAbsent(schema, e -> ConcurrentHashMap.newKeySet()).add(edge);
        to.ins().putNonRecursive(edge);
    }

    @Override
    public void putNonRecursive(EDGE edge) {
        edges.computeIfAbsent(edge.schema(), e -> ConcurrentHashMap.newKeySet()).add(edge);
    }

    @Override
    public void deleteNonRecursive(EDGE edge) {
        if (edges.containsKey(edge.schema())) edges.get(edge.schema()).remove(edge);
    }

    @Override
    public void deleteAll() {
        for (EDGE_SCHEMA schema : schemaValues()) delete(schema);
    }

    @Override
    public void forEach(Consumer<EDGE> function) {
        edges.forEach((key, set) -> set.forEach(function));
    }

    protected static class IteratorBuilderImpl<
            ITER_EDGE extends Edge<?, ?, ITER_VERTEX>,
            ITER_VERTEX extends Vertex<?, ?, ITER_VERTEX, ?, ITER_EDGE>
            > implements Adjacency.IteratorBuilder<ITER_VERTEX> {

        protected final Iterator<ITER_EDGE> edgeIterator;

        protected IteratorBuilderImpl(Iterator<ITER_EDGE> edgeIterator) {
            this.edgeIterator = edgeIterator;
        }

        @Override
        public Iterator<ITER_VERTEX> to() {
            return Iterators.apply(edgeIterator, Edge::to);
        }

        @Override
        public Iterator<ITER_VERTEX> from() {
            return Iterators.apply(edgeIterator, Edge::from);
        }
    }

    protected static abstract class Buffered<
            BUF_EDGE_SCHEMA extends Schema.Edge,
            BUF_EDGE extends Edge<BUF_EDGE_SCHEMA, ?, BUF_VERTEX>,
            BUF_VERTEX extends Vertex<?, ?, BUF_VERTEX, BUF_EDGE_SCHEMA, BUF_EDGE>,
            BUF_ITER_BUILDER extends IteratorBuilderImpl<BUF_EDGE, BUF_VERTEX>
            > extends AdjacencyImpl<BUF_EDGE_SCHEMA, BUF_EDGE, BUF_VERTEX, BUF_ITER_BUILDER> {

        protected Buffered(BUF_VERTEX owner, Direction direction) {
            super(owner, direction);
        }

        @Override
        public BUF_ITER_BUILDER edge(BUF_EDGE_SCHEMA schema) {
            Set<BUF_EDGE> t;
            if ((t = edges.get(schema)) != null) return newIteratorBuilder(t.iterator());
            return newIteratorBuilder(Collections.emptyIterator());
        }

        @Override
        public BUF_EDGE edge(BUF_EDGE_SCHEMA schema, BUF_VERTEX adjacent) {
            if (edges.containsKey(schema)) {
                Predicate<BUF_EDGE> predicate = direction.isOut()
                        ? e -> e.to().equals(adjacent)
                        : e -> e.from().equals(adjacent);
                return edges.get(schema).stream().filter(predicate).findAny().orElse(null);
            }
            return null;
        }

        @Override
        public void delete(BUF_EDGE_SCHEMA schema, BUF_VERTEX adjacent) {
            if (edges.containsKey(schema)) {
                Predicate<BUF_EDGE> predicate = direction.isOut()
                        ? e -> e.to().equals(adjacent)
                        : e -> e.from().equals(adjacent);
                edges.get(schema).stream().filter(predicate).forEach(Edge::delete);
            }
        }

        @Override
        public void delete(BUF_EDGE_SCHEMA schema) {
            if (edges.containsKey(schema)) edges.get(schema).forEach(Edge::delete);
        }
    }

    protected static abstract class Persisted<
            PER_EDGE_SCHEMA extends Schema.Edge,
            PER_EDGE extends Edge<PER_EDGE_SCHEMA, ?, PER_VERTEX>,
            PER_VERTEX extends Vertex<?, ?, PER_VERTEX, PER_EDGE_SCHEMA, PER_EDGE>,
            PER_ITER_BUILDER extends IteratorBuilderImpl<PER_EDGE, PER_VERTEX>
            > extends AdjacencyImpl<PER_EDGE_SCHEMA, PER_EDGE, PER_VERTEX, PER_ITER_BUILDER> {

        protected Persisted(PER_VERTEX owner, Direction direction) {
            super(owner, direction);
        }

        protected abstract PER_EDGE newPersistedEdge(Graph<?, PER_VERTEX> graph, byte[] key, byte[] value);

        @Override
        public PER_ITER_BUILDER edge(PER_EDGE_SCHEMA schema) {
            Iterator<PER_EDGE> storageIterator = owner.graph().storage().iterate(
                    join(owner.iid().bytes(), direction.isOut() ? schema.out().bytes() : schema.in().bytes()),
                    (key, value) -> newPersistedEdge(owner.graph(), key, value)
            );

            if (edges.get(schema) == null) {
                return newIteratorBuilder(storageIterator);
            } else {
                return newIteratorBuilder(link(edges.get(schema).iterator(), storageIterator));
            }
        }

        @Override
        public PER_EDGE edge(PER_EDGE_SCHEMA schema, PER_VERTEX adjacent) {
            Optional<PER_EDGE> container;
            Predicate<PER_EDGE> predicate = direction.isOut()
                    ? e -> e.to().equals(adjacent)
                    : e -> e.from().equals(adjacent);

            if (edges.containsKey(schema) &&
                    (container = edges.get(schema).stream().filter(predicate).findAny()).isPresent()) {
                return container.get();
            } else {
                Schema.Infix infix = direction.isOut() ? schema.out() : schema.in();
                byte[] edgeIID = join(owner.iid().bytes(), infix.bytes(), adjacent.iid().bytes());
                byte[] overriddenIID;
                if ((overriddenIID = owner.graph().storage().get(edgeIID)) != null) {
                    return newPersistedEdge(owner.graph(), edgeIID, overriddenIID);
                }
            }

            return null;
        }

        @Override
        public void delete(PER_EDGE_SCHEMA schema, PER_VERTEX adjacent) {
            Optional<PER_EDGE> container;
            Predicate<PER_EDGE> predicate = direction.isOut()
                    ? e -> e.to().equals(adjacent)
                    : e -> e.from().equals(adjacent);

            if (edges.containsKey(schema) &&
                    (container = edges.get(schema).stream().filter(predicate).findAny()).isPresent()) {
                edges.get(schema).remove(container.get());
            } else {
                Schema.Infix infix = direction.isOut() ? schema.out() : schema.in();
                byte[] edgeIID = join(owner.iid().bytes(), infix.bytes(), adjacent.iid().bytes());
                byte[] overriddenIID;
                if ((overriddenIID = owner.graph().storage().get(edgeIID)) != null) {
                    (newPersistedEdge(owner.graph(), edgeIID, overriddenIID)).delete();
                }
            }
        }

        @Override
        public void delete(PER_EDGE_SCHEMA schema) {
            if (edges.containsKey(schema)) edges.get(schema).parallelStream().forEach(Edge::delete);
            Iterator<PER_EDGE> storageIterator = owner.graph().storage().iterate(
                    join(owner.iid().bytes(), direction.isOut() ? schema.out().bytes() : schema.in().bytes()),
                    (key, value) -> newPersistedEdge(owner.graph(), key, value)
            );
            storageIterator.forEachRemaining(Edge::delete);
        }
    }
}
