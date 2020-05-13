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

import hypergraph.graph.util.Schema;
import hypergraph.graph.TypeGraph;
import hypergraph.graph.edge.TypeEdge;
import hypergraph.graph.vertex.TypeVertex;

import javax.annotation.Nullable;

import static java.util.Arrays.copyOfRange;

/**
 * A Type Edge that connects two Type Vertices, and an overridden Type Vertex.
 */
public class TypeEdgeImpl {

    /**
     * A Buffered Type Edge that connects two Type Vertices, and an overridden Type Vertex.
     */
    public static class Buffered extends EdgeImpl.Buffered<TypeGraph, Schema.Edge.Type, TypeEdge, TypeVertex> implements TypeEdge {

        private TypeVertex overridden;

        public Buffered(TypeGraph graph, Schema.Edge.Type schema, TypeVertex from, TypeVertex to) {
            super(graph, schema, from, to);
        }

        @Override
        TypeEdgeImpl.Buffered getThis() {
            return this;
        }

        /**
         * @return type vertex overridden by the head of this type edge
         */
        @Override
        public TypeVertex overridden() {
            return overridden;
        }

        /**
         * Set the head type vertex of this type edge to override a given type vertex.
         *
         * @param overridden the type vertex to override by the head
         */
        @Override
        public void overridden(TypeVertex overridden) {
            this.overridden = overridden;
        }

        /**
         * Delete operation of a buffered type edge.
         *
         * The delete operation involves removing this type edge from the {@code from.outs()} and
         * {@code to.ins()} edge collections.
         */
        @Override
        public void delete() {
            from.outs().deleteNonRecursive(this);
            to.ins().deleteNonRecursive(this);
        }

        /**
         * Commit operation of a buffered type edge.
         *
         * This operation can only be performed oncec, and thus protected by {@code committed} boolean.
         * Then we check for each direction of this edge, whether they need to be persisted to storage.
         * It's possible that an edge only has a {@code schema.out()} (most likely an optimisation edge)
         * and therefore will not have an inward edge to be persisted onto storage.
         */
        @Override
        public void commit() {
            if (committed.compareAndSet(false, true)) {
                if (schema.out() != null) {
                    if (overridden != null) graph.storage().put(outIID(), overridden.iid());
                    else graph.storage().put((outIID()));
                }
                if (schema.in() != null) graph.storage().put(inIID());
            }
        }
    }

    /**
     * Persisted Type Edge that connects two Type Vertices, and an overridden Type Vertex
     */
    public static class Persisted extends EdgeImpl.Persisted<TypeGraph, Schema.Edge.Type, TypeEdge, TypeVertex> implements TypeEdge {

        private byte[] overriddenIID;
        private TypeVertex overridden;

        public Persisted(TypeGraph graph, byte[] iid, @Nullable byte[] overriddenIID) {
            super(graph, Schema.Edge.Type.of(iid[Schema.IID.TYPE.length()]), Schema.IID.TYPE, iid);

            if (Schema.Edge.isOut(iid[Schema.IID.TYPE.length()])) {
                this.overriddenIID = overriddenIID;
            } else {
                this.overriddenIID = null;
                assert overriddenIID == null || overriddenIID.length == 0;
            }
        }

        /**
         * @return type vertex overridden by the head of this type edge
         */
        @Override
        public TypeVertex overridden() {
            if (overridden != null) return overridden;
            if (overriddenIID == null || overriddenIID.length == 0) return null;

            overridden = graph.get(overriddenIID);
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
            graph.storage().put(outIID, overriddenIID);
        }

        @Override
        TypeEdgeImpl.Persisted getThis() {
            return this;
        }
    }
}
