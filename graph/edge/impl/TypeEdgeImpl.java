/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.graph.edge.impl;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.graph.TypeGraph;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.edge.TypeEdge;
import com.vaticle.typedb.core.graph.iid.EdgeIID;
import com.vaticle.typedb.core.graph.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.hash;

/**
 * A Type Edge that connects two Type Vertices, and an overridden Type Vertex.
 */
public abstract class TypeEdgeImpl implements TypeEdge {

    final TypeGraph graph;
    final Encoding.Edge.Type encoding;

    TypeEdgeImpl(TypeGraph graph, Encoding.Edge.Type encoding) {
        this.graph = graph;
        this.encoding = encoding;
    }

    /**
     * A Buffered Type Edge that connects two Type Vertices, and an overridden Type Vertex.
     */
    public static class Buffered extends TypeEdgeImpl implements TypeEdge {

        private final TypeVertex from;
        private final TypeVertex to;
        private final AtomicBoolean committed;
        private final AtomicBoolean deleted;
        private final ByteArray outIIDBytes;
        private TypeVertex overridden;
        private int hash;

        /**
         * Default constructor for {@code EdgeImpl.Buffered}.
         *
         * @param from     the tail vertex
         * @param encoding the edge {@code Encoding}
         * @param to       the head vertex
         */
        public Buffered(Encoding.Edge.Type encoding, TypeVertex from, TypeVertex to) {
            super(from.graph(), encoding);
            assert this.graph == to.graph();
            this.from = from;
            this.to = to;
            this.outIIDBytes = outIID().bytes();
            committed = new AtomicBoolean(false);
            deleted = new AtomicBoolean(false);
        }

        @Override
        public Encoding.Edge.Type encoding() {
            return encoding;
        }

        @Override
        public EdgeIID.Type outIID() {
            return EdgeIID.Type.of(from().iid(), encoding.out(), to().iid());
        }

        @Override
        public EdgeIID.Type inIID() {
            return EdgeIID.Type.of(to().iid(), encoding.in(), from().iid());
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
                from.outs().remove(this);
                to.ins().remove(this);
                if (from instanceof Persisted && to instanceof Persisted) {
                    graph.storage().deleteUntracked(outIID().bytes());
                    graph.storage().deleteUntracked(inIID().bytes());
                }
            }
        }

        /**
         * Commit operation of a buffered type edge.
         *
         * This operation can only be performed once, and thus protected by {@code committed} boolean.
         * Then we check for each direction of this edge, whether they need to be persisted to storage.
         * It's possible that an edge only has a {@code encoding.out()} (most likely an optimisation edge)
         * and therefore will not have an inward edge to be persisted onto storage.
         */
        @Override
        public void commit() {
            if (committed.compareAndSet(false, true)) {
                if (encoding.out() != null) {
                    if (overridden != null) graph.storage().putUntracked(outIID().bytes(), overridden.iid().bytes());
                    else graph.storage().putUntracked(outIID().bytes());
                }
                if (encoding.in() != null) {
                    graph.storage().putUntracked(inIID().bytes());
                }
            }
        }

        /**
         * Determine the equality of a {@code TypeEdgeImpl.Buffered} against another.
         *
         * We only use {@code encoding}, {@code from} and {@code to} as the are
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
            return (this.encoding.equals(that.encoding) &&
                    this.from.equals(that.from) &&
                    this.to.equals(that.to));
        }

        /**
         * Determine the equality of a {@code Edge.Buffered} against another.
         *
         * We only use {@code encoding}, {@code from} and {@code to} as the are
         * the fixed properties that do not change, unlike {@code overridden}.
         * They are also the canonical properties required to uniquely identify
         * a {@code TypeEdgeImpl.Buffered}.
         *
         * @return int of the hashcode
         */
        @Override
        public final int hashCode() {
            if (hash == 0) hash = hash(encoding, from, to);
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
        private final ByteArray persistedBytes;
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
            super(graph, iid.encoding());

            if (iid.isOutwards()) {
                fromIID = iid.start();
                toIID = iid.end();
                outIID = iid;
                inIID = EdgeIID.Type.of(iid.end(), iid.encoding().in(), iid.start());
            } else {
                fromIID = iid.end();
                toIID = iid.start();
                inIID = iid;
                outIID = EdgeIID.Type.of(iid.end(), iid.encoding().out(), iid.start());
            }

            persistedBytes = iid.bytes();
            deleted = new AtomicBoolean(false);

            if (iid.isOutwards()) {
                this.overriddenIID = overriddenIID;
            } else {
                this.overriddenIID = null;
                assert overriddenIID == null;
            }
        }

        @Override
        public Encoding.Edge.Type encoding() {
            return encoding;
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
            from.outs().cache(this);
            return from;
        }

        @Override
        public TypeVertex to() {
            if (to != null) return to;
            to = graph.convert(toIID);
            to.ins().cache(this);
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
            graph.storage().putUntracked(outIID.bytes(), overriddenIID.bytes());
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
                from().outs().remove(this);
                to().ins().remove(this);
                graph.storage().deleteUntracked(this.outIID.bytes());
                graph.storage().deleteUntracked(this.inIID.bytes());
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
         * We only use {@code encoding}, {@code fromIID} and {@code toIID} as the
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
            return (this.encoding.equals(that.encoding) &&
                    this.fromIID.equals(that.fromIID) &&
                    this.toIID.equals(that.toIID));
        }

        /**
         * HashCode of a {@code TypeEdgeImpl.Persisted}.
         *
         * We only use {@code encoding}, {@code fromIID} and {@code toIID} as the
         * are the fixed properties that do not change, unlike
         * {@code overriddenIID} and {@code isDeleted}. They are also the
         * canonical properties required to uniquely identify an
         * {@code TypeEdgeImpl.Persisted}.
         *
         * @return int of the hashcode
         */
        @Override
        public final int hashCode() {
            if (hash == 0) hash = hash(encoding, fromIID.hashCode(), toIID.hashCode());
            return hash;
        }
    }
}
