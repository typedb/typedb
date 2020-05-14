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

import hypergraph.graph.Graph;
import hypergraph.graph.edge.Edge;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.Vertex;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static hypergraph.common.collection.ByteArrays.join;
import static java.util.Arrays.copyOfRange;
import static java.util.Objects.hash;

public abstract class EdgeImpl<
        GRAPH extends Graph<VERTEX>,
        EDGE_SCHEMA extends Schema.Edge,
        EDGE extends Edge<EDGE_SCHEMA, VERTEX>,
        VERTEX extends Vertex<?, VERTEX, EDGE_SCHEMA, EDGE>> implements Edge<EDGE_SCHEMA, VERTEX> {

    protected final GRAPH graph;
    protected final EDGE_SCHEMA schema;

    public EdgeImpl(GRAPH graph, EDGE_SCHEMA schema) {
        this.graph = graph;
        this.schema = schema;
    }

    abstract EDGE getThis();

    @Override
    public EDGE_SCHEMA schema() {
        return schema;
    }

    @Override
    public abstract boolean equals(Object object);

    @Override
    public abstract int hashCode();

    public static abstract class Buffered<
            GRAPH extends Graph<VERTEX>,
            EDGE_SCHEMA extends Schema.Edge,
            EDGE extends Edge<EDGE_SCHEMA, VERTEX>,
            VERTEX extends Vertex<?, VERTEX, EDGE_SCHEMA, EDGE>> extends EdgeImpl<GRAPH, EDGE_SCHEMA, EDGE, VERTEX> {

        protected final GRAPH graph;
        protected final AtomicBoolean committed;
        protected final VERTEX from;
        protected final VERTEX to;

        /**
         * Default constructor for {@code EdgeImpl.Buffered}.
         *
         * @param graph  the graph comprised of all the vertices
         * @param schema the edge {@code Schema}
         * @param from   the tail vertex
         * @param to     the head vertex
         */
        public Buffered(GRAPH graph, EDGE_SCHEMA schema, VERTEX from, VERTEX to) {
            super(graph, schema);
            this.graph = graph;
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
         * @return the {@code iid} of this edge pointing outwards
         */
        @Override
        public byte[] outIID() {
            return join(from().iid(), schema.out().key(), to().iid());
        }

        /**
         * @return the {@code iid} of this edge pointing inwards
         */
        @Override
        public byte[] inIID() {
            return join(to().iid(), schema.in().key(), from().iid());
        }

        /**
         * @return the tail vertex of this edge
         */
        @Override
        public VERTEX from() {
            return from;
        }

        /**
         * @return the head vertex of this edge
         */
        @Override
        public VERTEX to() {
            return to;
        }

        /**
         * Determine the equality of a {@code EdgeImpl.Buffered} against another.
         *
         * We only use {@code schema}, {@code from} and {@code to} as the are the fixed properties
         * that do not change, unlike {@code overridden}. They are also the canonical properties
         * required to identify a {@code EdgeImpl.Buffered} uniquely.
         *
         * @param object that we want to compare against
         * @return true if equal, else false
         */
        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            EdgeImpl.Buffered that = (EdgeImpl.Buffered) object;
            return (this.schema.equals(that.schema) &&
                    this.from.equals(that.from) &&
                    this.to.equals(that.to));
        }

        /**
         * Determine the equality of a {@code Edge.Buffered} against another.
         *
         * We only use {@code schema}, {@code from} and {@code to} as the are the fixed properties
         * that do not change, unlike {@code overridden}. They are also the canonical properties
         * required to identify a {@code Edge.Buffered} uniquely.
         *
         * @return int of the hashcode
         */
        @Override
        public final int hashCode() {
            return hash(schema, from, to);
        }
    }

    public static abstract class Persisted<
            GRAPH extends Graph<VERTEX>,
            EDGE_SCHEMA extends Schema.Edge,
            EDGE extends Edge<EDGE_SCHEMA, VERTEX>,
            VERTEX extends Vertex<?, VERTEX, EDGE_SCHEMA, EDGE>> extends EdgeImpl<GRAPH, EDGE_SCHEMA, EDGE, VERTEX> {

        protected final byte[] outIID;
        protected final byte[] inIID;
        protected final byte[] fromIID;
        protected final byte[] toIID;
        protected final AtomicBoolean isDeleted;

        protected VERTEX from;
        protected VERTEX to;

        /**
         * Default constructor for {@code Edge.Persisted}.
         *
         * The edge can be constructed from an {@code iid} that represents either an inwards
         * or outwards pointing edge. Thus, we extract the {@code start} and {@code end} of it,
         * and use the {@code infix} of the edge {@code iid} to determine the direction,
         * and which vertex becomes {@code fromIID} or {@code toIID}.
         *
         * The head of this edge may or may not be overriding another vertex.
         * If it does the {@code overriddenIID} will not be null.
         *
         * @param graph the graph comprised of all the vertices
         * @param iid   the {@code iid} of a persisted edge
         */
        public Persisted(GRAPH graph, EDGE_SCHEMA edgeSchema, Schema.IID iidSchema, byte[] iid) {
            super(graph, edgeSchema);
            byte[] start = copyOfRange(iid, 0, iidSchema.length());
            byte[] end = copyOfRange(iid, iidSchema.length() + 1, iid.length);

            if (Schema.Edge.isOut(iid[iidSchema.length()])) {
                fromIID = start;
                toIID = end;
                outIID = iid;
                inIID = join(end, edgeSchema.in().key(), start);
            } else {
                fromIID = end;
                toIID = start;
                inIID = iid;
                outIID = join(end, schema().out().key(), start);
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
         * @return the {@code iid} of this edge pointing outwards
         */
        @Override
        public byte[] outIID() {
            return outIID;
        }

        /**
         * @return the {@code iid} of this edge pointing inwards
         */
        @Override
        public byte[] inIID() {
            return inIID;
        }

        /**
         * @return the tail vertex of this edge
         */
        @Override
        public VERTEX from() {
            if (from != null) return from;
            from = graph.get(fromIID);
            return from;
        }

        /**
         * @return the head vertex of this edge
         */
        @Override
        public VERTEX to() {
            if (to != null) return to;
            to = graph.get(toIID);
            return to;
        }

        /**
         * Delete operation of a persisted edge.
         *
         * This operation can only be performed once, and thus protected by {@code isDelete} boolean.
         * The delete operation involves removing this edge from the {@code from.outs()} and
         * {@code to.ins()} edge collections, and then delete both directions of this edge
         * from the graph storage.
         */
        @Override
        public void delete() {
            if (isDeleted.compareAndSet(false, true)) {
                if (from != null) from.outs().deleteNonRecursive(getThis());
                if (to != null) to.ins().deleteNonRecursive(getThis());
                graph.storage().delete(this.outIID);
                graph.storage().delete(this.inIID);
            }
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

        /**
         * Determine the equality of a {@code Edge} against another.
         *
         * We only use {@code schema}, {@code fromIID} and {@code toIID} as the are the fixed properties
         * that do not change, unlike {@code overriddenIID} and {@code isDeleted}. They are also the
         * canonical properties required to identify a {@code EdgeImpl.Persisted} uniquely.
         *
         * @param object that that we want to compare against
         * @return true if equal, else false
         */
        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            EdgeImpl.Persisted that = (EdgeImpl.Persisted) object;
            return (this.schema.equals(that.schema) &&
                    Arrays.equals(this.fromIID, that.fromIID) &&
                    Arrays.equals(this.toIID, that.toIID));
        }

        /**
         * HashCode of a {@code EdgeImpl.Persisted}.
         *
         * We only use {@code schema}, {@code fromIID} and {@code toIID} as the are the fixed properties
         * that do not change, unlike {@code overriddenIID} and {@code isDeleted}. They are also the
         * canonical properties required to identify a {@code EdgeImpl.Persisted} uniquely.
         *
         * @return int of the hashcode
         */
        @Override
        public final int hashCode() {
            return hash(schema, Arrays.hashCode(fromIID), Arrays.hashCode(toIID));
        }
    }
}
