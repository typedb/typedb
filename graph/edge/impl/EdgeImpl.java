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
import hypergraph.graph.iid.EdgeIID;
import hypergraph.graph.iid.VertexIID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.Vertex;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.hash;

public abstract class EdgeImpl {

    static abstract class Buffered<
            EDGE_SCHEMA extends Schema.Edge,
            EDGE_IID extends EdgeIID<EDGE_SCHEMA, ?, ?>,
            EDGE extends Edge<EDGE_SCHEMA, EDGE_IID, VERTEX>,
            VERTEX extends Vertex<?, ?, VERTEX, EDGE_SCHEMA, EDGE>> {

        final EDGE_SCHEMA schema;
        final VERTEX from;
        final VERTEX to;
        final AtomicBoolean committed;
        final AtomicBoolean deleted;
        private int hash;

        /**
         * Default constructor for {@code EdgeImpl.Buffered}.
         *
         * @param schema the edge {@code Schema}
         * @param from   the tail vertex
         * @param to     the head vertex
         */
        Buffered(EDGE_SCHEMA schema, VERTEX from, VERTEX to) {
            this.schema = schema;
            this.from = from;
            this.to = to;
            committed = new AtomicBoolean(false);
            deleted = new AtomicBoolean(false);
        }

        abstract EDGE getThis();

        abstract EDGE_IID edgeIID(VERTEX start, Schema.Infix infix, VERTEX end);

        public EDGE_SCHEMA schema() {
            return schema;
        }

        public Schema.Status status() {
            return committed.get() ? Schema.Status.COMMITTED : Schema.Status.BUFFERED;
        }

        public EDGE_IID outIID() {
            return edgeIID(from(), schema.out(), to());
        }

        public EDGE_IID inIID() {
            return edgeIID(to(), schema.in(), from());
        }

        public VERTEX from() {
            return from;
        }

        public VERTEX to() {
            return to;
        }

        /**
         * Deletes this {@code Edge} from connecting between two {@code Vertex}.
         *
         * A {@code EdgeImpl.Buffered} can only exist in the adjacency cache of
         * each {@code Vertex}, and does not exist in storage.
         */
        public void delete() {
            if (deleted.compareAndSet(false, true)) {
                from.outs().deleteNonRecursive(getThis());
                to.ins().deleteNonRecursive(getThis());
            }
        }

        /**
         * Determine the equality of a {@code EdgeImpl.Buffered} against another.
         *
         * We only use {@code schema}, {@code from} and {@code to} as the are
         * the fixed properties that do not change, unlike {@code overridden}.
         * They are also the canonical properties required to uniquely identify
         * a {@code EdgeImpl.Buffered} uniquely.
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
         * We only use {@code schema}, {@code from} and {@code to} as the are
         * the fixed properties that do not change, unlike {@code overridden}.
         * They are also the canonical properties required to uniquely identify
         * a {@code EdgeImpl.Buffered}.
         *
         * @return int of the hashcode
         */
        @Override
        public final int hashCode() {
            if (hash == 0) hash = hash(schema, from, to);
            return hash;
        }
    }

    static abstract class Persisted<
            GRAPH extends Graph<VERTEX_IID, VERTEX>,
            EDGE_SCHEMA extends Schema.Edge,
            EDGE_IID extends EdgeIID<EDGE_SCHEMA, VERTEX_IID, VERTEX_IID>,
            EDGE extends Edge<EDGE_SCHEMA, EDGE_IID, VERTEX>,
            VERTEX_IID extends VertexIID,
            VERTEX extends Vertex<VERTEX_IID, ?, VERTEX, EDGE_SCHEMA, EDGE>> {

        final GRAPH graph;
        final EDGE_SCHEMA schema;
        final EDGE_IID outIID;
        final EDGE_IID inIID;
        final VERTEX_IID fromIID;
        final VERTEX_IID toIID;
        final AtomicBoolean deleted;
        VERTEX from;
        VERTEX to;
        private int hash;

        /**
         * Default constructor for {@code Edge.Persisted}.
         *
         * The edge can be constructed from an {@code iid} that represents
         * either an inwards or outwards pointing edge. Thus, we extract the
         * {@code start} and {@code end} of it, and use the {@code infix} of the
         * edge {@code iid} to determine the direction, and which vertex becomes
         * {@code fromIID} or {@code toIID}.
         *
         * The head of this edge may or may not be overriding another vertex.
         * If it does the {@code overriddenIID} will not be null.
         *
         * @param graph the graph comprised of all the vertices
         * @param iid   the {@code iid} of a persisted edge
         */
        Persisted(GRAPH graph, EDGE_IID iid) {
            this.graph = graph;
            this.schema = iid.schema();

            if (iid.isOutwards()) {
                fromIID = iid.start();
                toIID = iid.end();
                outIID = iid;
                inIID = edgeIID(iid.end(), iid.schema().in(), iid.start());
            } else {
                fromIID = iid.end();
                toIID = iid.start();
                inIID = iid;
                outIID = edgeIID(iid.end(), iid.schema().out(), iid.start());
            }

            deleted = new AtomicBoolean(false);
        }

        abstract EDGE getThis();

        abstract EDGE_IID edgeIID(VERTEX_IID start, Schema.Infix infix, VERTEX_IID end);

        public EDGE_SCHEMA schema() {
            return schema;
        }

        public Schema.Status status() {
            return Schema.Status.PERSISTED;
        }

        public EDGE_IID outIID() {
            return outIID;
        }

        public EDGE_IID inIID() {
            return inIID;
        }

        public VERTEX from() {
            if (from != null) return from;
            from = graph.convert(fromIID);
            from.outs().putNonRecursive(getThis());
            return from;
        }

        public VERTEX to() {
            if (to != null) return to;
            to = graph.convert(toIID);
            to.ins().putNonRecursive(getThis());
            return to;
        }

        /**
         * Delete operation of a persisted edge.
         *
         * This operation can only be performed once, and thus protected by
         * {@code isDelete} atomic boolean. The delete operation involves
         * removing this edge from the {@code from.outs()} and {@code to.ins()}
         * edge collections in case it is cached. Then, delete both directions
         * of this edge from the graph storage.
         */
        public void delete() {
            if (deleted.compareAndSet(false, true)) {
                from().outs().deleteNonRecursive(getThis());
                to().ins().deleteNonRecursive(getThis());
                graph.storage().delete(this.outIID.bytes());
                graph.storage().delete(this.inIID.bytes());
            }
        }

        /**
         * Determine the equality of a {@code Edge} against another.
         *
         * We only use {@code schema}, {@code fromIID} and {@code toIID} as the
         * are the fixed properties that do not change, unlike
         * {@code overriddenIID} and {@code isDeleted}. They are also the
         * canonical properties required to identify a {@code Persisted} edge.
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
         * We only use {@code schema}, {@code fromIID} and {@code toIID} as the
         * are the fixed properties that do not change, unlike
         * {@code overriddenIID} and {@code isDeleted}. They are also the
         * canonical properties required to uniquely identify an
         * {@code EdgeImpl.Persisted}.
         *
         * @return int of the hashcode
         */
        @Override
        public final int hashCode() {
            if (hash == 0) hash = hash(schema, fromIID.hashCode(), toIID.hashCode());
            return hash;
        }
    }
}
