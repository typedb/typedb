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

package hypergraph.graph.edge;

import hypergraph.graph.Graph;
import hypergraph.graph.Schema;
import hypergraph.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static hypergraph.common.collection.ByteArrays.join;
import static java.util.Arrays.copyOfRange;
import static java.util.Objects.hash;

/**
 * A Type Edge that connects two Type Vertices, and an overridden Type Vertex.
 */
public abstract class TypeEdge extends Edge<Schema.Edge.Type, TypeVertex> {

    protected final Graph.Type graph;

    TypeEdge(Graph.Type graph, Schema.Edge.Type schema) {
        super(schema);
        this.graph = graph;
    }

    /**
     * @return type vertex overridden by the head of this type edge.
     */
    public abstract TypeVertex overridden();

    /**
     * Set the head type vertex of this type edge to override a given type vertex.
     *
     * @param overridden the type vertex to override by the head
     */
    public abstract void overridden(TypeVertex overridden);

    /**
     * A Buffered Type Edge that connects two Type Vertices, and an overridden Type Vertex.
     */
    public static class Buffered extends TypeEdge {

        private final AtomicBoolean committed;
        private final TypeVertex from;
        private final TypeVertex to;
        private TypeVertex overridden;

        /**
         * Default constructor for {@code TypeEdge.Buffered}.
         *
         * @param graph  the type graph, comprised of type vertices
         * @param schema the type edge {@code schema}
         * @param from   the tail type vertex
         * @param to     the head type vertex
         */
        public Buffered(Graph.Type graph, Schema.Edge.Type schema, TypeVertex from, TypeVertex to) {
            super(graph, schema);
            this.from = from;
            this.to = to;
            committed = new AtomicBoolean(false);
        }

        /**
         * @return the status of this edge: either {@code committed} or {@code buffered}
         */
        @Override
        public Schema.Status status() {
            return committed.get() ? Schema.Status.COMMITTED : Schema.Status.BUFFERED;
        }

        /**
         * @return the {@code iid} of this type edge pointing outwards
         */
        @Override
        public byte[] outIID() {
            return join(from().iid(), schema.out().key(), to().iid());
        }

        /**
         * @return the {@code iid} of this type edge pointing inwards
         */
        @Override
        public byte[] inIID() {
            return join(to().iid(), schema.in().key(), from().iid());
        }

        /**
         * @return the tail type vertex of this type edge
         */
        @Override
        public TypeVertex from() {
            return from;
        }

        /**
         * @return the head type vertex of this type edge
         */
        @Override
        public TypeVertex to() {
            return to;
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

        /**
         * Determine the equality of a {@code TypeEdge.Buffered} against another.
         *
         * We only use {@code schema}, {@code from} and {@code to} as the are the fixed properties
         * that do not change, unlike {@code overridden}. They are also the canonical properties
         * required to identify a {@code TypeEdge.Buffered} uniquely.
         *
         * @param object that we want to compare against
         * @return true if equal, else false
         */
        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            TypeEdge.Buffered that = (TypeEdge.Buffered) object;
            return (this.schema.equals(that.schema) &&
                    this.from.equals(that.from) &&
                    this.to.equals(that.to));
        }

        /**
         * Determine the equality of a {@code TypeEdge.Buffered} against another.
         *
         * We only use {@code schema}, {@code from} and {@code to} as the are the fixed properties
         * that do not change, unlike {@code overridden}. They are also the canonical properties
         * required to identify a {@code TypeEdge.Buffered} uniquely.
         *
         * @return int of the hashcode
         */
        @Override
        public final int hashCode() {
            return hash(schema, from, to);
        }
    }

    /**
     * Persisted Type Edge that connects two Type Vertices, and an overridden Type Vertex
     */
    public static class Persisted extends TypeEdge {

        private final byte[] outIID;
        private final byte[] inIID;
        private final byte[] fromIID;
        private final byte[] toIID;
        private final AtomicBoolean isDeleted;

        private TypeVertex from;
        private TypeVertex to;
        private TypeVertex overridden;
        private byte[] overriddenIID;

        /**
         * Default constructor for {@code TypeEdge.Persisted}.
         *
         * The edge can be constructed from an {@code iid} that represents either an inwards
         * or outwards pointing edge. Thus, we extract the {@code start} and {@code end} of it,
         * and use the {@code infix} of the edge {@code iid} to determine the direction,
         * and which vertex becomes {@code fromIID} or {@code toIID}.
         *
         * The head of this edge may or may not be overriding another type vertex.
         * If it does the {@code overriddenIID} will not be null.
         *
         * @param graph         the type graph, comprised of type vertices
         * @param iid           the {@code iid} of a persisted type edge
         * @param overriddenIID the {@code iid} of the head vertex overrides
         */
        public Persisted(Graph.Type graph, byte[] iid, @Nullable byte[] overriddenIID) {
            super(graph, Schema.Edge.Type.of(iid[Schema.IID.TYPE.length()]));
            byte[] start = copyOfRange(iid, 0, Schema.IID.TYPE.length());
            byte[] end = copyOfRange(iid, Schema.IID.TYPE.length() + 1, iid.length);

            if (Schema.Edge.isOut(iid[Schema.IID.TYPE.length()])) {
                fromIID = start; toIID = end; outIID = iid;
                inIID = join(end, schema.in().key(), start);
                this.overriddenIID = overriddenIID;
            } else {
                fromIID = end; toIID = start; inIID = iid;
                outIID = join(end, schema().out().key(), start);
                this.overriddenIID = null;
            }

            isDeleted = new AtomicBoolean(false);
        }

        /**
         * @return the status of this edge: {@code persisted}
         */
        @Override
        public Schema.Status status() {
            return Schema.Status.PERSISTED;
        }

        /**
         * @return the {@code iid} of this type edge pointing outwards
         */
        @Override
        public byte[] outIID() {
            return outIID;
        }

        /**
         * @return the {@code iid} of this type edge pointing inwards
         */
        @Override
        public byte[] inIID() {
            return inIID;
        }

        /**
         * @return the tail type vertex of this type edge
         */
        @Override
        public TypeVertex from() {
            if (from != null) return from;
            from = graph.get(fromIID);
            return from;
        }

        /**
         * @return the head type vertex of this type edge
         */
        @Override
        public TypeVertex to() {
            if (to != null) return to;
            to = graph.get(toIID);
            return to;
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

        /**
         * Delete operation of a persisted type edge.
         *
         * This operation can only be performed once, and thus protected by {@code isDelete} boolean.
         * The delete operation involves removing this type edge from the {@code from.outs()} and
         * {@code to.ins()} edge collections, and then delete both directions of this edge
         * from the graph storage.
         */
        @Override
        public void delete() {
            if (isDeleted.compareAndSet(false, true)) {
                if (from != null) from.outs().deleteNonRecursive(this);
                if (to != null) to.ins().deleteNonRecursive(this);
                graph.storage().delete(this.outIID);
                graph.storage().delete(this.inIID);
            }
        }

        /**
         * No-op commit operation of a persisted type edge.
         *
         * Persisted type edges do not need to be committed back to the graph storage.
         * The only property of a persisted type edge that can be changed is only the
         * {@code overriddenIID}, and that is immediately written to storage when changed.
         */
        @Override
        public void commit() {}

        /**
         * Determine the equality of a {@code TypeEdge} against another.
         *
         * We only use {@code schema}, {@code fromIID} and {@code toIID} as the are the fixed properties
         * that do not change, unlike {@code overriddenIID} and {@code isDeleted}. They are also the
         * canonical properties required to identify a {@code TypeEdge} uniquely.
         *
         * @param object that that we want to compare against
         * @return true if equal, else false
         */
        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            TypeEdge.Persisted that = (TypeEdge.Persisted) object;
            return (this.schema.equals(that.schema) &&
                    Arrays.equals(this.fromIID, that.fromIID) &&
                    Arrays.equals(this.toIID, that.toIID));
        }

        /**
         * HashCode of a {@code TypeEdge}.
         *
         * We only use {@code schema}, {@code fromIID} and {@code toIID} as the are the fixed properties
         * that do not change, unlike {@code overriddenIID} and {@code isDeleted}. They are also the
         * canonical properties required to identify a {@code TypeEdge} uniquely.
         *
         * @return int of the hashcode
         */
        @Override
        public final int hashCode() {
            return hash(schema, Arrays.hashCode(fromIID), Arrays.hashCode(toIID));
        }
    }
}
