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
import hypergraph.graph.adjacency.TypeAdjacency;
import hypergraph.graph.edge.TypeEdge;
import hypergraph.graph.edge.impl.TypeEdgeImpl;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.TypeVertexImpl;

import java.util.Iterator;

public class TypeAdjacencyImpl {

    public static class TypeIteratorBuilderImpl
            extends AdjacencyImpl.IteratorBuilderImpl<TypeEdge, TypeVertexImpl>
            implements TypeAdjacency.TypeIteratorBuilder {

        TypeIteratorBuilderImpl(Iterator<TypeEdge> edgeIterator) {
            super(edgeIterator);
        }

        public Iterator<TypeVertexImpl> overridden() {
            return Iterators.apply(edgeIterator, TypeEdge::overridden);
        }
    }

    public static class Buffered
            extends AdjacencyImpl.Buffered<Schema.Edge.Type, TypeEdge, TypeVertexImpl, TypeIteratorBuilderImpl>
            implements TypeAdjacency {

        public Buffered(TypeVertexImpl owner, Direction direction) {
            super(owner, direction);
        }

        @Override
        protected Schema.Edge.Type[] schemaValues() {
            return Schema.Edge.Type.values();
        }

        @Override
        protected TypeIteratorBuilderImpl newIteratorBuilder(Iterator<TypeEdge> typeEdgeIterator) {
            return new TypeIteratorBuilderImpl(typeEdgeIterator);
        }

        @Override
        protected TypeEdge newBufferedEdge(Schema.Edge.Type schema, TypeVertexImpl from, TypeVertexImpl to) {
            return new TypeEdgeImpl.Buffered(owner.graph(), schema, from, to);
        }

    }

    public static class Persisted
            extends AdjacencyImpl.Persisted<Schema.Edge.Type, TypeEdge, TypeVertexImpl, TypeIteratorBuilderImpl>
            implements TypeAdjacency {

        public Persisted(TypeVertexImpl owner, Direction direction) {
            super(owner, direction);
        }

        @Override
        protected Schema.Edge.Type[] schemaValues() {
            return Schema.Edge.Type.values();
        }

        @Override
        protected TypeIteratorBuilderImpl newIteratorBuilder(Iterator<TypeEdge> typeEdgeIterator) {
            return new TypeIteratorBuilderImpl(typeEdgeIterator);
        }

        @Override
        protected TypeEdge newBufferedEdge(Schema.Edge.Type schema, TypeVertexImpl from, TypeVertexImpl to) {
            return new TypeEdgeImpl.Buffered(owner.graph(), schema, from, to);
        }

        @Override
        protected TypeEdge newPersistedEdge(Graph<TypeVertexImpl> graph, byte[] key, byte[] value) {
            return new TypeEdgeImpl.Persisted(owner.graph(), key, value);
        }
    }
}
