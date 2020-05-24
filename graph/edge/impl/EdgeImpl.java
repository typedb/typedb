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
import hypergraph.graph.util.IID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.Vertex;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.hash;

public abstract class EdgeImpl<
        GRAPH extends Graph<?, VERTEX>,
        EDGE_IID extends IID.Edge<EDGE_SCHEMA, VERTEX_IID>,
        EDGE_SCHEMA extends Schema.Edge,
        EDGE extends Edge<EDGE_IID, EDGE_SCHEMA, VERTEX>,
        VERTEX_IID extends IID.Vertex,
        VERTEX extends Vertex<VERTEX_IID, ?, VERTEX, EDGE_SCHEMA, EDGE>
        > implements Edge<EDGE_IID, EDGE_SCHEMA, VERTEX> {

    protected final GRAPH graph;
    protected final EDGE_SCHEMA schema;

    public EdgeImpl(GRAPH graph, EDGE_SCHEMA schema) {
        this.graph = graph;
        this.schema = schema;
    }

    abstract EDGE getThis();

    abstract EDGE_IID edgeIID(VERTEX_IID start, Schema.Infix infix, VERTEX_IID end);

    @Override
    public EDGE_SCHEMA schema() {
        return schema;
    }

    @Override
    public abstract boolean equals(Object object);

    @Override
    public abstract int hashCode();

    public static abstract class Buffered<
            GRAPH extends Graph<?, VERTEX>,
            EDGE_IID extends IID.Edge<EDGE_SCHEMA, VERTEX_IID>,
            EDGE_SCHEMA extends Schema.Edge,
            EDGE extends Edge<EDGE_IID, EDGE_SCHEMA, VERTEX>,
            VERTEX_IID extends IID.Vertex,
            VERTEX extends Vertex<VERTEX_IID, ?, VERTEX, EDGE_SCHEMA, EDGE>
            > extends EdgeImpl<GRAPH, EDGE_IID, EDGE_SCHEMA, EDGE, VERTEX_IID, VERTEX> {

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
            this.from = from;
            this.to = to;
            committed = new AtomicBoolean(false);
        }

        /**
         * Returns the status of this edge: either {@code committed} or {@code buffered}.
         *
         * @return the status of this edge: either {@code committed} or {@code buffered}
         */
        @Override
        public Schema.Status status() {
            return committed.get() ? Schema.Status.COMMITTED : Schema.Status.BUFFERED;
        }

        /**
         * Returns the {@code iid} of this edge pointing outwards.
         *
         * @return the {@code iid} of this edge pointing outwards
         */
        @Override
        public EDGE_IID outIID() {
            return edgeIID(from().iid(), schema.out(), to().iid());
        }

        /**
         * Returns the {@code iid} of this edge pointing inwards.
         *
         * @return the {@code iid} of this edge pointing inwards
         */
        @Override
        public EDGE_IID inIID() {
            return edgeIID(to().iid(), schema.in(), from().iid());
        }

        /**
         * Returns the tail vertex of this edge.
         *
         * @return the tail vertex of this edge
         */
        @Override
        public VERTEX from() {
            return from;
        }

        /**
         * Returns the head vertex of this edge.
         *
         * @return the head vertex of this edge
         */
        @Override
        public VERTEX to() {
            return to;
        }

        /**
         * Delete operation of a buffered edge.
         *
         * The delete operation involves removing this edge from the {@code from.outs()} and
         * {@code to.ins()} edge collections.
         */
        @Override
        public void delete() {
            from.outs().deleteNonRecursive(getThis());
            to.ins().deleteNonRecursive(getThis());
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
            GRAPH extends Graph<VERTEX_IID, VERTEX>,
            EDGE_IID extends IID.Edge<EDGE_SCHEMA, VERTEX_IID>,
            EDGE_SCHEMA extends Schema.Edge,
            EDGE extends Edge<EDGE_IID, EDGE_SCHEMA, VERTEX>,
            VERTEX_IID extends IID.Vertex,
            VERTEX extends Vertex<VERTEX_IID, ?, VERTEX, EDGE_SCHEMA, EDGE>
            > extends EdgeImpl<GRAPH, EDGE_IID, EDGE_SCHEMA, EDGE, VERTEX_IID, VERTEX> {

        protected final EDGE_IID outIID;
        protected final EDGE_IID inIID;
        protected final VERTEX_IID fromIID;
        protected final VERTEX_IID toIID;
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
        public Persisted(GRAPH graph, EDGE_IID iid) {
            super(graph, iid.schema());
            VERTEX_IID start = iid.start();
            VERTEX_IID end = iid.end();

            if (iid.isOutwards()) {
                fromIID = start;
                toIID = end;
                outIID = iid;
                inIID = edgeIID(end, iid.schema().in(), start);
            } else {
                fromIID = end;
                toIID = start;
                inIID = iid;
                outIID = edgeIID(end, iid.schema().out(), start);
            }

            isDeleted = new AtomicBoolean(false);
        }

        /**
         * Returns the status of this edge: {@code persisted}.
         *
         * @return the status of this edge: {@code persisted}
         */
        @Override
        public Schema.Status status() {
            return Schema.Status.PERSISTED;
        }

        /**
         * Returns the {@code iid} of this edge pointing outwards.
         *
         * @return the {@code iid} of this edge pointing outwards
         */
        @Override
        public EDGE_IID outIID() {
            return outIID;
        }

        /**
         * Returns the {@code iid} of this edge pointing inwards.
         *
         * @return the {@code iid} of this edge pointing inwards
         */
        @Override
        public EDGE_IID inIID() {
            return inIID;
        }

        /**
         * Returns the tail vertex of this edge.
         *
         * @return the tail vertex of this edge
         */
        @Override
        public VERTEX from() {
            if (from != null) return from;
            from = graph.convert(fromIID);
            return from;
        }

        /**
         * Returns the head vertex of this edge.
         *
         * @return the head vertex of this edge
         */
        @Override
        public VERTEX to() {
            if (to != null) return to;
            to = graph.convert(toIID);
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
                graph.storage().delete(this.outIID.bytes());
                graph.storage().delete(this.inIID.bytes());
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
                    this.fromIID.equals(that.fromIID) &&
                    this.toIID.equals(that.toIID));
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
            return hash(schema, fromIID.hashCode(), toIID.hashCode());
        }
    }
}
