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

package hypergraph.graph.edge.impl;

import hypergraph.common.iterator.Iterators;
import hypergraph.graph.edge.Edge;
import hypergraph.graph.edge.EdgeMap;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.Vertex;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

public abstract class EdgeMapImpl<
        EDGE_SCHEMA extends Schema.Edge,
        EDGE extends Edge<EDGE_SCHEMA, VERTEX>,
        VERTEX extends Vertex> implements EdgeMap<EDGE_SCHEMA, EDGE, VERTEX> {

    protected final VERTEX owner;
    protected final Direction direction;
    protected final ConcurrentMap<EDGE_SCHEMA, Set<EDGE>> edges;

    protected EdgeMapImpl(VERTEX owner, Direction direction) {
        this.owner = owner;
        this.direction = direction;
        this.edges = new ConcurrentHashMap<>();
    }

    public static class IteratorBuilderImpl<
            ITER_VERTEX extends Vertex,
            ITER_EDGE extends Edge<?, ITER_VERTEX>> implements EdgeMap.IteratorBuilder<ITER_VERTEX> {

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

    @Override
    public void putNonRecursive(EDGE edge) {
        edges.computeIfAbsent(edge.schema(), e -> ConcurrentHashMap.newKeySet()).add(edge);
    }

    @Override
    public void forEach(Consumer<EDGE> function) {
        edges.forEach((key, set) -> set.forEach(function));
    }
}
