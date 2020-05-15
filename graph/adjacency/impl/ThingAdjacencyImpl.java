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

import hypergraph.graph.Graph;
import hypergraph.graph.adjacency.Adjacency;
import hypergraph.graph.adjacency.ThingAdjacency;
import hypergraph.graph.edge.ThingEdge;
import hypergraph.graph.edge.impl.ThingEdgeImpl;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.impl.ThingVertexImpl;

import java.util.Iterator;

public abstract class ThingAdjacencyImpl {

    public static class ThingIteratorBuilderImpl
            extends AdjacencyImpl.IteratorBuilderImpl<ThingEdge, ThingVertexImpl>
            implements Adjacency.IteratorBuilder<ThingVertexImpl> {

        ThingIteratorBuilderImpl(Iterator<ThingEdge> edgeIterator) {
            super(edgeIterator);
        }
    }

    public static class Buffered
            extends AdjacencyImpl.Buffered<Schema.Edge.Thing, ThingEdge, ThingVertexImpl, ThingIteratorBuilderImpl>
            implements ThingAdjacency {

        protected Buffered(ThingVertexImpl owner, Direction direction) {
            super(owner, direction);
        }

        @Override
        protected Schema.Edge.Thing[] schemaValues() {
            return Schema.Edge.Thing.values();
        }

        @Override
        protected ThingIteratorBuilderImpl newIteratorBuilder(Iterator<ThingEdge> thingEdgeIterator) {
            return new ThingIteratorBuilderImpl(thingEdgeIterator);
        }

        @Override
        protected ThingEdge newBufferedEdge(Schema.Edge.Thing schema, ThingVertexImpl from, ThingVertexImpl to) {
            return new ThingEdgeImpl.Buffered(owner.graph(), schema, from, to);
        }
    }

    public static class Persisted
            extends AdjacencyImpl.Persisted<Schema.Edge.Thing, ThingEdge, ThingVertexImpl, ThingIteratorBuilderImpl>
            implements ThingAdjacency {


        protected Persisted(ThingVertexImpl owner, Direction direction) {
            super(owner, direction);
        }

        @Override
        protected Schema.Edge.Thing[] schemaValues() {
            return Schema.Edge.Thing.values();
        }

        @Override
        protected ThingIteratorBuilderImpl newIteratorBuilder(Iterator<ThingEdge> thingEdgeIterator) {
            return new ThingIteratorBuilderImpl(thingEdgeIterator);
        }

        @Override
        protected ThingEdge newBufferedEdge(Schema.Edge.Thing schema, ThingVertexImpl from, ThingVertexImpl to) {
            return new ThingEdgeImpl.Buffered(owner.graph(), schema, from, to);
        }

        /**
         * Instantiate a new persisted edge by creating a new {@code ThingEdge.Persisted}
         *
         * For {@code ThingEdgeImpl.Persisted} we ignore the {@code value} that the {@code key} ({@code iid})
         * points to, as edges between {@code ThingVertex} never stores anything as {@code value}
         *
         * @param graph the graph containing the {@code ThingVertex}
         * @param key   the edge {@code iid}
         * @param value the value that the edge {@code iid} points to in the storage
         * @return a new {@code ThingEdge}
         */
        @Override
        protected ThingEdge newPersistedEdge(Graph<ThingVertexImpl> graph, byte[] key, byte[] value) {
            return new ThingEdgeImpl.Persisted(owner.graph(), key);
        }
    }
}
