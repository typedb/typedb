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

import hypergraph.graph.TypeGraph;
import hypergraph.graph.edge.TypeEdge;
import hypergraph.graph.util.IID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.TypeVertex;

import javax.annotation.Nullable;

/**
 * A Type Edge that connects two Type Vertices, and an overridden Type Vertex.
 */
public class TypeEdgeImpl {

    /**
     * A Buffered Type Edge that connects two Type Vertices, and an overridden Type Vertex.
     */
    public static class Buffered
            extends EdgeImpl.Buffered<Schema.Edge.Type, IID.Edge.Type, TypeEdge, TypeVertex>
            implements TypeEdge {

        private final TypeGraph graph;
        private TypeVertex overridden;

        public Buffered(Schema.Edge.Type schema, TypeVertex from, TypeVertex to) {
            super(schema, from, to);
            this.graph = from.graph();
            assert this.graph == to.graph();
        }

        @Override
        TypeEdgeImpl.Buffered getThis() {
            return this;
        }

        @Override
        IID.Edge.Type edgeIID(TypeVertex start, Schema.Infix infix, TypeVertex end) {
            return IID.Edge.Type.of(start.iid(), infix, end.iid());
        }

        @Override
        public TypeVertex overridden() {
            return overridden;
        }

        @Override
        public void overridden(TypeVertex overridden) {
            this.overridden = overridden;
        }

        /**
         * Commit operation of a buffered type edge.
         *
         * This operation can only be performed once, and thus protected by {@code committed} boolean.
         * Then we check for each direction of this edge, whether they need to be persisted to storage.
         * It's possible that an edge only has a {@code schema.out()} (most likely an optimisation edge)
         * and therefore will not have an inward edge to be persisted onto storage.
         */
        @Override
        public void commit() {
            if (committed.compareAndSet(false, true)) {
                if (schema.out() != null) {
                    if (overridden != null) graph.storage().put(outIID().bytes(), overridden.iid().bytes());
                    else graph.storage().put((outIID().bytes()));
                }
                if (schema.in() != null) graph.storage().put(inIID().bytes());
            }
        }
    }

    /**
     * Persisted Type Edge that connects two Type Vertices, and an overridden Type Vertex
     */
    public static class Persisted
            extends EdgeImpl.Persisted<TypeGraph, Schema.Edge.Type, IID.Edge.Type, TypeEdge, IID.Vertex.Type, TypeVertex>
            implements TypeEdge {

        private IID.Vertex.Type overriddenIID;
        private TypeVertex overridden;

        public Persisted(TypeGraph graph, IID.Edge.Type iid, @Nullable IID.Vertex.Type overriddenIID) {
            super(graph, iid);

            if (iid.isOutwards()) {
                this.overriddenIID = overriddenIID;
            } else {
                this.overriddenIID = null;
                assert overriddenIID == null;
            }
        }

        @Override
        TypeEdgeImpl.Persisted getThis() {
            return this;
        }

        @Override
        IID.Edge.Type edgeIID(IID.Vertex.Type start, Schema.Infix infix, IID.Vertex.Type end) {
            return IID.Edge.Type.of(start, infix, end);
        }

        @Override
        public TypeVertex overridden() {
            if (overridden != null) return overridden;
            if (overriddenIID == null) return null;

            overridden = graph.convert(overriddenIID);
            return overridden;
        }

        /**
         * Set the head type vertex of this type edge to override a given type vertex.
         *
         * Once the property has been set, we write to storage immediately as this type edge
         * does not buffer information in memory before being persisted.
         *
         * @param overridden the type vertex to override by the head
         */
        @Override
        public void overridden(TypeVertex overridden) {
            this.overridden = overridden;
            overriddenIID = overridden.iid();
            graph.storage().put(outIID.bytes(), overriddenIID.bytes());
        }

        /**
         * No-op commit operation of a persisted edge.
         *
         * Persisted edges do not need to be committed back to the graph storage.
         * The only property of a persisted edge that can be changed is only the
         * {@code overriddenIID}, and that is immediately written to storage when changed.
         */
        @Override
        public void commit() {}
    }
}
