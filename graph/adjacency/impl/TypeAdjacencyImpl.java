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
import hypergraph.graph.adjacency.TypeAdjacency;
import hypergraph.graph.edge.TypeEdge;
import hypergraph.graph.edge.impl.TypeEdgeImpl;
import hypergraph.graph.util.IID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.TypeVertex;

import java.util.Iterator;

public class TypeAdjacencyImpl {

    private static class Util extends AdjacencyImpl.Util<Schema.Edge.Type, TypeEdge, TypeVertex, TypeIteratorBuilderImpl> {

        @Override
        protected Schema.Edge.Type[] schemaValues() {
            return Schema.Edge.Type.values();
        }

        @Override
        protected TypeIteratorBuilderImpl newIteratorBuilder(Iterator<TypeEdge> typeEdgeIterator) {
            return new TypeIteratorBuilderImpl(typeEdgeIterator);
        }

        @Override
        protected TypeEdge newBufferedEdge(Schema.Edge.Type schema, TypeVertex from, TypeVertex to) {
            return new TypeEdgeImpl.Buffered(schema, from, to);
        }
    }

    public static class TypeIteratorBuilderImpl
            extends AdjacencyImpl.IteratorBuilderImpl<TypeEdge, TypeVertex>
            implements TypeAdjacency.TypeIteratorBuilder {

        TypeIteratorBuilderImpl(Iterator<TypeEdge> edgeIterator) {
            super(edgeIterator);
        }

        public Iterator<TypeVertex> overridden() {
            return Iterators.apply(edgeIterator, TypeEdge::overridden);
        }
    }

    public static class Buffered
            extends AdjacencyImpl.Buffered<Schema.Edge.Type, TypeEdge, TypeVertex, TypeIteratorBuilderImpl>
            implements TypeAdjacency {

        public Buffered(TypeVertex owner, Direction direction) {
            super(owner, direction, new TypeAdjacencyImpl.Util());
        }
    }

    public static class Persisted
            extends AdjacencyImpl.Persisted<Schema.Edge.Type, TypeEdge, TypeVertex, TypeIteratorBuilderImpl>
            implements TypeAdjacency {

        public Persisted(TypeVertex owner, Direction direction) {
            super(owner, direction, new TypeAdjacencyImpl.Util());
        }

        @Override
        protected TypeEdge newPersistedEdge(byte[] key, byte[] value) {
            IID.Vertex.Type overridden = (value == null || value.length == 0) ? null : IID.Vertex.Type.of(value);
            return new TypeEdgeImpl.Persisted(owner.graph(), IID.Edge.Type.of(key), overridden);
        }
    }
}
