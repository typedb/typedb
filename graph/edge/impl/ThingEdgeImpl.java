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

import hypergraph.graph.ThingGraph;
import hypergraph.graph.edge.Edge;
import hypergraph.graph.edge.ThingEdge;
import hypergraph.graph.iid.EdgeIID;
import hypergraph.graph.iid.VertexIID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.ThingVertex;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.hash;

public abstract class ThingEdgeImpl implements Edge<Schema.Edge.Thing, ThingVertex> {

    final ThingGraph graph;
    final Schema.Edge.Thing schema;

    ThingEdgeImpl(ThingGraph graph, Schema.Edge.Thing schema) {
        this.graph = graph;
        this.schema = schema;
    }

    public static class Buffered extends ThingEdgeImpl implements ThingEdge {

        private final ThingVertex from;
        private final ThingVertex to;
        private final AtomicBoolean committed;
        private final AtomicBoolean deleted;
        private final VertexIID.Type metadata;
        private int hash;

        /**
         * Default constructor for {@code ThingEdgeImpl.Buffered}.
         *
         * @param from   the tail vertex
         * @param schema the edge {@code Schema}
         * @param to     the head vertex
         */
        public Buffered(ThingVertex from, Schema.Edge.Thing schema, @Nullable VertexIID.Type metadata, ThingVertex to) {
            super(from.graph(), schema);
            assert this.graph == to.graph();
            this.from = from;
            this.to = to;
            this.metadata = metadata;
            committed = new AtomicBoolean(false);
            deleted = new AtomicBoolean(false);
        }

        @Override
        public Schema.Edge.Thing schema() {
            return schema;
        }

        @Override
        public ThingVertex from() {
            return from;
        }

        @Override
        public ThingVertex to() {
            return to;
        }

        @Override
        public boolean hasMetaData(VertexIID.Type metadata) {
            return this.metadata != null && this.metadata.equals(metadata);
        }

        /**
         * Deletes this {@code Edge} from connecting between two {@code Vertex}.
         *
         * A {@code ThingEdgeImpl.Buffered} can only exist in the adjacency cache of
         * each {@code Vertex}, and does not exist in storage.
         */
        @Override
        public void delete() {
            if (deleted.compareAndSet(false, true)) {
                from.outs().deleteNonRecursive(this);
                to.ins().deleteNonRecursive(this);
            }
        }

        @Override
        public void commit() {
            if (committed.compareAndSet(false, true)) {
                if (schema.out() != null) {
                    graph.storage().put(EdgeIID.Thing.of(from().iid(), schema.out(), to().iid()).bytes());
                }
                if (schema.in() != null) {
                    graph.storage().put(EdgeIID.Thing.of(to().iid(), schema.in(), from().iid()).bytes());
                }
            }
        }

        /**
         * Determine the equality of a {@code ThingEdgeImpl.Buffered} against another.
         *
         * We only use {@code schema}, {@code from} and {@code to} as the are
         * the fixed properties that do not change, unlike {@code overridden}.
         * They are also the canonical properties required to uniquely identify
         * a {@code ThingEdgeImpl.Buffered} uniquely.
         *
         * @param object that we want to compare against
         * @return true if equal, else false
         */
        @Override
        public final boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            ThingEdgeImpl.Buffered that = (ThingEdgeImpl.Buffered) object;
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
         * a {@code ThingEdgeImpl.Buffered}.
         *
         * @return int of the hashcode
         */
        @Override
        public final int hashCode() {
            if (hash == 0) hash = hash(schema, from, to);
            return hash;
        }
    }

    public static class Persisted implements ThingEdge {

        private final ThingGraph graph;
        private final Schema.Edge.Thing schema;
        private final EdgeIID.Thing outIID;
        private final EdgeIID.Thing inIID;
        private final VertexIID.Thing fromIID;
        private final VertexIID.Thing toIID;
        private final AtomicBoolean deleted;
        private ThingVertex from;
        private ThingVertex to;
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
        public Persisted(ThingGraph graph, EdgeIID.Thing iid) {
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

        ThingEdgeImpl.Persisted getThis() {
            return this;
        }


        EdgeIID.Thing edgeIID(VertexIID.Thing start, Schema.Infix infix, VertexIID.Thing end) {
            return EdgeIID.Thing.of(start, infix, end);
        }

        @Override
        public Schema.Edge.Thing schema() {
            return schema;
        }

        @Override
        public ThingVertex from() {
            if (from != null) return from;
            from = graph.convert(fromIID);
            from.outs().load(getThis());
            return from;
        }

        @Override
        public ThingVertex to() {
            if (to != null) return to;
            to = graph.convert(toIID);
            to.ins().load(getThis());
            return to;
        }

        @Override
        public boolean hasMetaData(VertexIID.Type metadata) {
            return outIID.infix().containsMetaData(metadata);
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
        @Override
        public void delete() {
            if (deleted.compareAndSet(false, true)) {
                from().outs().deleteNonRecursive(getThis());
                to().ins().deleteNonRecursive(getThis());
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
         * We only use {@code schema}, {@code fromIID} and {@code toIID} as the
         * are the fixed properties that do not change, unlike
         * {@code overriddenIID} and {@code isDeleted}. They are also the
         * canonical properties required to identify a {@code Persisted} edge.
         *
         * @param object that that we want to compare against
         * @return true if equal, else false
         */
        @Override
        public final boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            ThingEdgeImpl.Persisted that = (ThingEdgeImpl.Persisted) object;
            return (this.schema.equals(that.schema) &&
                    this.fromIID.equals(that.fromIID) &&
                    this.toIID.equals(that.toIID));
        }

        /**
         * HashCode of a {@code ThingEdgeImpl.Persisted}.
         *
         * We only use {@code schema}, {@code fromIID} and {@code toIID} as the
         * are the fixed properties that do not change, unlike
         * {@code overriddenIID} and {@code isDeleted}. They are also the
         * canonical properties required to uniquely identify an
         * {@code ThingEdgeImpl.Persisted}.
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
