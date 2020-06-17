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

import hypergraph.graph.adjacency.Adjacency;
import hypergraph.graph.edge.Edge;
import hypergraph.graph.iid.InfixIID;
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
import static hypergraph.common.iterator.Iterators.apply;
import static hypergraph.common.iterator.Iterators.distinct;
import static hypergraph.common.iterator.Iterators.link;

public abstract class AdjacencyImpl<
        EDGE_SCHEMA extends Schema.Edge,
        EDGE extends Edge<EDGE_SCHEMA, ?, VERTEX>,
        VERTEX extends Vertex<?, ?, VERTEX, EDGE_SCHEMA, EDGE>,
        ITER_BUILDER extends AdjacencyImpl.IteratorBuilderImpl<EDGE, VERTEX>
        > implements Adjacency<EDGE_SCHEMA, EDGE, VERTEX> {

    final VERTEX owner;
    final Direction direction;
    final ConcurrentMap<InfixIID, Set<EDGE>> edges;
    final Util<EDGE_SCHEMA, EDGE, VERTEX, ITER_BUILDER> util;

    AdjacencyImpl(VERTEX owner, Direction direction, Util<EDGE_SCHEMA, EDGE, VERTEX, ITER_BUILDER> util) {
        this.owner = owner;
        this.direction = direction;
        this.edges = new ConcurrentHashMap<>();
        this.util = util;
    }

    InfixIID infixIID(EDGE_SCHEMA prefix) {
        Schema.Infix infix = direction.isOut() ? prefix.out() : prefix.in();
        if (infix != null) return InfixIID.of(infix);
        return null;
    }

    @Override
    public void put(EDGE_SCHEMA schema, VERTEX adjacent) {
        VERTEX from = direction.isOut() ? owner : adjacent;
        VERTEX to = direction.isOut() ? adjacent : owner;
        EDGE edge = util.newBufferedEdge(from, schema, to);
        edges.computeIfAbsent(infixIID(schema), e -> ConcurrentHashMap.newKeySet()).add(edge);
        to.ins().putNonRecursive(edge);
        owner.setModified();
    }

    @Override
    public void putNonRecursive(EDGE edge) {
        load(edge);
        owner.setModified();
    }

    @Override
    public void load(EDGE edge) {
        edges.computeIfAbsent(infixIID(edge.schema()), e -> ConcurrentHashMap.newKeySet()).add(edge);
    }

    @Override
    public void deleteNonRecursive(EDGE edge) {
        if (edges.containsKey(infixIID(edge.schema()))) {
            edges.get(infixIID(edge.schema())).remove(edge);
            owner.setModified();
        }
    }

    @Override
    public void deleteAll() {
        for (EDGE_SCHEMA schema : util.schemaValues()) delete(schema);
    }

    @Override
    public void forEach(Consumer<EDGE> function) {
        edges.forEach((key, set) -> set.forEach(function));
    }

    protected static abstract class Util<
            EDGE_SCHEMA extends Schema.Edge,
            EDGE extends Edge<EDGE_SCHEMA, ?, VERTEX>,
            VERTEX extends Vertex<?, ?, VERTEX, EDGE_SCHEMA, EDGE>,
            ITER_BUILDER extends AdjacencyImpl.IteratorBuilderImpl<EDGE, VERTEX>
            > {

        protected abstract EDGE_SCHEMA[] schemaValues();

        protected abstract ITER_BUILDER newIteratorBuilder(Iterator<EDGE> edgeIterator);

        protected abstract EDGE newBufferedEdge(VERTEX from, EDGE_SCHEMA schema, VERTEX to);
    }

    protected static abstract class IteratorBuilderImpl<
            EDGE extends Edge<?, ?, VERTEX>,
            VERTEX extends Vertex<?, ?, VERTEX, ?, EDGE>
            > implements Adjacency.IteratorBuilder<VERTEX> {

        final Iterator<EDGE> edgeIterator;

        IteratorBuilderImpl(Iterator<EDGE> edgeIterator) {
            this.edgeIterator = edgeIterator;
        }

        @Override
        public Iterator<VERTEX> to() {
            return apply(edgeIterator, Edge::to);
        }

        @Override
        public Iterator<VERTEX> from() {
            return apply(edgeIterator, Edge::from);
        }
    }

    protected static abstract class Buffered<
            EDGE_SCHEMA extends Schema.Edge,
            EDGE extends Edge<EDGE_SCHEMA, ?, VERTEX>,
            VERTEX extends Vertex<?, ?, VERTEX, EDGE_SCHEMA, EDGE>,
            ITER_BUILDER extends IteratorBuilderImpl<EDGE, VERTEX>
            > extends AdjacencyImpl<EDGE_SCHEMA, EDGE, VERTEX, ITER_BUILDER> {

        protected Buffered(VERTEX owner, Direction direction, Util<EDGE_SCHEMA, EDGE, VERTEX, ITER_BUILDER> util) {
            super(owner, direction, util);
        }

        @Override
        public ITER_BUILDER edge(EDGE_SCHEMA schema) {
            Set<EDGE> t;
            if ((t = edges.get(infixIID(schema))) != null) return util.newIteratorBuilder(t.iterator());
            return util.newIteratorBuilder(Collections.emptyIterator());
        }

        @Override
        public EDGE edge(EDGE_SCHEMA schema, VERTEX adjacent) {
            InfixIID infix = infixIID(schema);
            if (edges.containsKey(infix)) {
                Predicate<EDGE> predicate = direction.isOut()
                        ? e -> e.to().equals(adjacent)
                        : e -> e.from().equals(adjacent);
                return edges.get(infix).stream().filter(predicate).findAny().orElse(null);
            }
            return null;
        }

        @Override
        public void delete(EDGE_SCHEMA schema, VERTEX adjacent) {
            InfixIID infix = infixIID(schema);
            if (edges.containsKey(infix)) {
                Predicate<EDGE> predicate = direction.isOut()
                        ? e -> e.to().equals(adjacent)
                        : e -> e.from().equals(adjacent);
                edges.get(infix).stream().filter(predicate).forEach(Edge::delete);
            }
        }

        @Override
        public void delete(EDGE_SCHEMA schema) {
            InfixIID infix = infixIID(schema);
            if (infix != null && edges.containsKey(infix)) edges.get(infix).forEach(Edge::delete);
        }
    }

    protected static abstract class Persisted<
            EDGE_SCHEMA extends Schema.Edge,
            EDGE extends Edge<EDGE_SCHEMA, ?, VERTEX>,
            VERTEX extends Vertex<?, ?, VERTEX, EDGE_SCHEMA, EDGE>,
            ITER_BUILDER extends IteratorBuilderImpl<EDGE, VERTEX>
            > extends AdjacencyImpl<EDGE_SCHEMA, EDGE, VERTEX, ITER_BUILDER> {

        protected Persisted(VERTEX owner, Direction direction, Util<EDGE_SCHEMA, EDGE, VERTEX, ITER_BUILDER> util) {
            super(owner, direction, util);
        }

        abstract EDGE newPersistedEdge(byte[] key, byte[] value);

        @Override
        public ITER_BUILDER edge(EDGE_SCHEMA schema) {
            InfixIID infix = infixIID(schema);
            Iterator<EDGE> storageIterator = owner.graph().storage().iterate(
                    join(owner.iid().bytes(), direction.isOut() ? schema.out().bytes() : schema.in().bytes()),
                    this::newPersistedEdge
            );

            if (edges.get(infix) == null) {
                return util.newIteratorBuilder(storageIterator);
            } else {
                return util.newIteratorBuilder(distinct(link(edges.get(infix).iterator(), storageIterator)));
            }
        }

        @Override
        public EDGE edge(EDGE_SCHEMA schema, VERTEX adjacent) {
            InfixIID infix = infixIID(schema);
            Optional<EDGE> container;
            Predicate<EDGE> predicate = direction.isOut()
                    ? e -> e.to().equals(adjacent)
                    : e -> e.from().equals(adjacent);

            if (edges.containsKey(infix) && (container = edges.get(infix).stream().filter(predicate).findAny()).isPresent()) {
                return container.get();
            } else {
                byte[] edgeIID = join(owner.iid().bytes(), infix.bytes(), adjacent.iid().bytes());
                byte[] overriddenIID;
                if ((overriddenIID = owner.graph().storage().get(edgeIID)) != null) {
                    return newPersistedEdge(edgeIID, overriddenIID);
                }
            }

            return null;
        }

        @Override
        public void delete(EDGE_SCHEMA schema, VERTEX adjacent) {
            InfixIID infix = infixIID(schema);
            Optional<EDGE> edgeOpt;
            Predicate<EDGE> predicate = direction.isOut()
                    ? e -> e.to().equals(adjacent)
                    : e -> e.from().equals(adjacent);

            if (edges.containsKey(infix) && (edgeOpt = edges.get(infix).stream().filter(predicate).findAny()).isPresent()) {
                edgeOpt.get().delete();
            }

            byte[] edgeIID = join(owner.iid().bytes(), infix.bytes(), adjacent.iid().bytes());
            byte[] iidValueBytes;
            if ((iidValueBytes = owner.graph().storage().get(edgeIID)) != null) {
                (newPersistedEdge(edgeIID, iidValueBytes)).delete();
            }
        }

        @Override
        public void delete(EDGE_SCHEMA schema) {
            InfixIID infix = infixIID(schema);
            if (edges.containsKey(infix)) edges.get(infix).parallelStream().forEach(Edge::delete);
            Iterator<EDGE> storageIterator = owner.graph().storage().iterate(
                    join(owner.iid().bytes(), direction.isOut() ? schema.out().bytes() : schema.in().bytes()),
                    this::newPersistedEdge
            );
            storageIterator.forEachRemaining(Edge::delete);
        }
    }
}
