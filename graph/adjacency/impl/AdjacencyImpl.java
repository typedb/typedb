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
import hypergraph.graph.adjacency.Adjacency;
import hypergraph.graph.edge.Edge;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.Vertex;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

public abstract class AdjacencyImpl<
        EDGE_SCHEMA extends Schema.Edge,
        EDGE extends Edge<EDGE_SCHEMA, VERTEX>,
        VERTEX extends Vertex<?, VERTEX, EDGE_SCHEMA, EDGE>
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

    protected abstract EDGE newTypeEdge(EDGE_SCHEMA schema, VERTEX from, VERTEX to);

    @Override
    public void put(EDGE_SCHEMA schema, VERTEX adjacent) {
        VERTEX from = direction.isOut() ? owner : adjacent;
        VERTEX to = direction.isOut() ? adjacent : owner;
        EDGE edge = newTypeEdge(schema, from, to);
        edges.computeIfAbsent(edge.schema(), e -> ConcurrentHashMap.newKeySet()).add(edge);
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
            ITER_EDGE extends Edge<?, ITER_VERTEX>,
            ITER_VERTEX extends Vertex
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
            BUF_EDGE extends Edge<BUF_EDGE_SCHEMA, BUF_VERTEX>,
            BUF_VERTEX extends Vertex<?, BUF_VERTEX, BUF_EDGE_SCHEMA, BUF_EDGE>
            > extends AdjacencyImpl<BUF_EDGE_SCHEMA, BUF_EDGE, BUF_VERTEX> {

        protected Buffered(BUF_VERTEX owner, Direction direction) {
            super(owner, direction);
        }
    }

    protected static abstract class Persisted<
            PER_EDGE_SCHEMA extends Schema.Edge,
            PER_EDGE extends Edge<PER_EDGE_SCHEMA, PER_VERTEX>,
            PER_VERTEX extends Vertex<?, PER_VERTEX, PER_EDGE_SCHEMA, PER_EDGE>
            > extends AdjacencyImpl<PER_EDGE_SCHEMA, PER_EDGE, PER_VERTEX> {

        protected Persisted(PER_VERTEX owner, Direction direction) {
            super(owner, direction);
        }
    }
}
