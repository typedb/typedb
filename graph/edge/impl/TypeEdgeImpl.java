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

package grakn.graph.edge.impl;

import grakn.graph.TypeGraph;
import grakn.graph.edge.TypeEdge;
import grakn.graph.iid.EdgeIID;
import grakn.graph.iid.VertexIID;
import grakn.graph.util.Schema;
import grakn.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.hash;

/**
 * A Type Edge that connects two Type Vertices, and an overridden Type Vertex.
 */
public abstract class TypeEdgeImpl implements TypeEdge {

    final TypeGraph graph;
    final Schema.Edge.Type schema;

    TypeEdgeImpl(TypeGraph graph, Schema.Edge.Type schema) {
        this.graph = graph;
        this.schema = schema;
    }

    /**
     * A Buffered Type Edge that connects two Type Vertices, and an overridden Type Vertex.
     */
    public static class Buffered extends TypeEdgeImpl implements TypeEdge {

        private final TypeVertex from;
        private final TypeVertex to;
        private final AtomicBoolean committed;
        private final AtomicBoolean deleted;
        private TypeVertex overridden;
        private int hash;

        /**
         * Default constructor for {@code EdgeImpl.Buffered}.
         *
         * @param from   the tail vertex
         * @param schema the edge {@code Schema}
         * @param to     the head vertex
         */
        public Buffered(Schema.Edge.Type schema, TypeVertex from, TypeVertex to) {
            super(from.graph(), schema);
            assert this.graph == to.graph();
            this.from = from;
            this.to = to;
            committed = new AtomicBoolean(false);
            deleted = new AtomicBoolean(false);
        }

        @Override
        public Schema.Edge.Type schema() {
            return schema;
        }

        @Override
        public EdgeIID.Type outIID() {
            return EdgeIID.Type.of(from().iid(), schema.out(), to().iid());
        }

        @Override
        public EdgeIID.Type inIID() {
            return EdgeIID.Type.of(to().iid(), schema.in(), from().iid());
        }

        @Override
        public TypeVertex from() {
            return from;
        }

        @Override
        public TypeVertex to() {
            return to;
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
         * Deletes this {@code Edge} from connecting between two {@code Vertex}.
         *
         * A {@code TypeEdgeImpl.Buffered} can only exist in the adjacency cache of
         * each {@code Vertex}, and does not exist in storage.
         */
        @Override
        public void delete() {
            if (deleted.compareAndSet(false, true)) {
                from.outs().removeFromBuffer(this);
                to.ins().removeFromBuffer(this);
                if (from instanceof Persisted && to instanceof Persisted) {
                    graph.storage().delete(outIID().bytes());
                    graph.storage().delete(inIID().bytes());
                }
            }
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
                    else graph.storage().put(outIID().bytes());
                }
                if (schema.in() != null) {
                    graph.storage().put(inIID().bytes());
                }
            }
        }

        /**
         * Determine the equality of a {@code TypeEdgeImpl.Buffered} against another.
         *
         * We only use {@code schema}, {@code from} and {@code to} as the are
         * the fixed properties that do not change, unlike {@code overridden}.
         * They are also the canonical properties required to uniquely identify
         * a {@code TypeEdgeImpl.Buffered} uniquely.
         *
         * @param object that we want to compare against
         * @return true if equal, else false
         */
        @Override
        public final boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            TypeEdgeImpl.Buffered that = (TypeEdgeImpl.Buffered) object;
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
         * a {@code TypeEdgeImpl.Buffered}.
         *
         * @return int of the hashcode
         */
        @Override
        public final int hashCode() {
            if (hash == 0) hash = hash(schema, from, to);
            return hash;
        }
    }

    /**
     * Persisted Type Edge that connects two Type Vertices, and an overridden Type Vertex
     */
    public static class Persisted extends TypeEdgeImpl implements TypeEdge {

        private final EdgeIID.Type outIID;
        private final EdgeIID.Type inIID;
        private final VertexIID.Type fromIID;
        private final VertexIID.Type toIID;
        private final AtomicBoolean deleted;
        private TypeVertex from;
        private TypeVertex to;
        private VertexIID.Type overriddenIID;
        private TypeVertex overridden;
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
        public Persisted(TypeGraph graph, EdgeIID.Type iid, @Nullable VertexIID.Type overriddenIID) {
            super(graph, iid.schema());

            if (iid.isOutwards()) {
                fromIID = iid.start();
                toIID = iid.end();
                outIID = iid;
                inIID = EdgeIID.Type.of(iid.end(), iid.schema().in(), iid.start());
            } else {
                fromIID = iid.end();
                toIID = iid.start();
                inIID = iid;
                outIID = EdgeIID.Type.of(iid.end(), iid.schema().out(), iid.start());
            }

            deleted = new AtomicBoolean(false);

            if (iid.isOutwards()) {
                this.overriddenIID = overriddenIID;
            } else {
                this.overriddenIID = null;
                assert overriddenIID == null;
            }
        }

        @Override
        public Schema.Edge.Type schema() {
            return schema;
        }

        @Override
        public EdgeIID.Type outIID() {
            return outIID;
        }

        @Override
        public EdgeIID.Type inIID() {
            return inIID;
        }

        @Override
        public TypeVertex from() {
            if (from != null) return from;
            from = graph.convert(fromIID);
            from.outs().loadToBuffer(this);
            return from;
        }

        @Override
        public TypeVertex to() {
            if (to != null) return to;
            to = graph.convert(toIID);
            to.ins().loadToBuffer(this);
            return to;
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
                from().outs().removeFromBuffer(this);
                to().ins().removeFromBuffer(this);
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
            TypeEdgeImpl.Persisted that = (TypeEdgeImpl.Persisted) object;
            return (this.schema.equals(that.schema) &&
                    this.fromIID.equals(that.fromIID) &&
                    this.toIID.equals(that.toIID));
        }

        /**
         * HashCode of a {@code TypeEdgeImpl.Persisted}.
         *
         * We only use {@code schema}, {@code fromIID} and {@code toIID} as the
         * are the fixed properties that do not change, unlike
         * {@code overriddenIID} and {@code isDeleted}. They are also the
         * canonical properties required to uniquely identify an
         * {@code TypeEdgeImpl.Persisted}.
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
