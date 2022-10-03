/*
 * Copyright (C) 2022 Vaticle
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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.graph.TypeGraph;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.graph.edge.TypeEdge;
import com.vaticle.typedb.core.encoding.iid.EdgeViewIID;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.ILLEGAL_OPERATION;
import static java.util.Objects.hash;

/**
 * A Type Edge that connects two Type Vertices, and an overridden Type Vertex.
 */
public abstract class TypeEdgeImpl implements TypeEdge {

    final TypeGraph graph;
    final Encoding.Edge.Type encoding;
    final View.Forward forward;
    final View.Backward backward;

    TypeEdgeImpl(TypeGraph graph, Encoding.Edge.Type encoding) {
        this.graph = graph;
        this.encoding = encoding;
        this.forward = new View.Forward(this);
        this.backward = new View.Backward(this);
    }

    @Override
    public View.Forward forwardView() {
        return forward;
    }

    @Override
    public View.Backward backwardView() {
        return backward;
    }

    abstract EdgeViewIID.Type computeForwardIID();

    abstract EdgeViewIID.Type computeBackwardIID();

    private static abstract class View<T extends TypeEdge.View<T>> implements TypeEdge.View<T> {

        final TypeEdgeImpl edge;
        EdgeViewIID.Type iidCache = null;

        private View(TypeEdgeImpl edge) {
            this.edge = edge;
        }

        @Override
        public TypeEdge edge() {
            return edge;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || this.getClass() != object.getClass()) return false;
            return edge.equals(((TypeEdgeImpl.View<?>) object).edge);
        }

        @Override
        public int hashCode() {
            return edge.hashCode();
        }

        private static class Forward extends TypeEdgeImpl.View<TypeEdge.View.Forward> implements TypeEdge.View.Forward {

            private Forward(TypeEdgeImpl edge) {
                super(edge);
            }

            @Override
            public EdgeViewIID.Type iid() {
                if (iidCache == null) iidCache = edge.computeForwardIID();
                return iidCache;
            }

            @Override
            public int compareTo(TypeEdge.View.Forward other) {
                return iid().compareTo(other.iid());
            }
        }

        private static class Backward extends TypeEdgeImpl.View<TypeEdge.View.Backward> implements TypeEdge.View.Backward {

            private Backward(TypeEdgeImpl edge) {
                super(edge);
            }

            @Override
            public EdgeViewIID.Type iid() {
                if (iidCache == null) iidCache = edge.computeBackwardIID();
                return iidCache;
            }

            @Override
            public int compareTo(TypeEdge.View.Backward other) {
                return iid().compareTo(other.iid());
            }
        }
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
         * @param from     the tail vertex
         * @param encoding the edge {@code Encoding}
         * @param to       the head vertex
         */
        public Buffered(Encoding.Edge.Type encoding, TypeVertex from, TypeVertex to) {
            super(from.graph(), encoding);
            assert this.graph == to.graph();
            this.from = from;
            this.to = to;
            committed = new AtomicBoolean(false);
            deleted = new AtomicBoolean(false);
        }

        @Override
        public Encoding.Edge.Type encoding() {
            return encoding;
        }

        @Override
        public EdgeViewIID.Type computeForwardIID() {
            return EdgeViewIID.Type.of(from().iid(), encoding.forward(), to().iid());
        }

        @Override
        public EdgeViewIID.Type computeBackwardIID() {
            return EdgeViewIID.Type.of(to().iid(), encoding.backward(), from().iid());
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
        public Optional<TypeVertex> overridden() {
            return Optional.ofNullable(overridden);
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
                    graph.storage().deleteUntracked(forward.iid());
                    graph.storage().deleteUntracked(backward.iid());
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
                // compute IID because vertices could have been committed
                if (overridden != null) {
                    graph.storage().putUntracked(computeForwardIID(), overridden.iid().bytes());
                    graph.storage().putUntracked(computeBackwardIID(), overridden.iid().bytes());
                } else {
                    graph.storage().putUntracked(computeForwardIID());
                    graph.storage().putUntracked(computeBackwardIID());
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


    public static class Target extends TypeEdgeImpl implements TypeEdge {

        private final TypeVertex from;
        private final TypeVertex to;
        private final int hash;

        public Target(Encoding.Edge.Type encoding, TypeVertex from, TypeVertex to) {
            super(from.graph(), encoding);
            this.from = from;
            this.to = to;
            this.hash = hash(Target.class, from, to);
        }

        @Override
        public Encoding.Edge.Type encoding() {
            return encoding;
        }

        @Override
        EdgeViewIID.Type computeForwardIID() {
            return EdgeViewIID.Type.of(from.iid(), encoding.forward(), to.iid());
        }

        @Override
        EdgeViewIID.Type computeBackwardIID() {
            return EdgeViewIID.Type.of(to.iid(), encoding.forward(), from.iid());
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
        public Optional<TypeVertex> overridden() {
            return Optional.empty();
        }

        @Override
        public void overridden(TypeVertex overridden) {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }

        @Override
        public void delete() {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }

        @Override
        public void commit() {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }

        @Override
        public final boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            TypeEdgeImpl.Target that = (TypeEdgeImpl.Target) object;
            return this.encoding.equals(that.encoding) &&
                    this.from.equals(that.from) &&
                    this.to.equals(that.to);
        }

        @Override
        public final int hashCode() {
            return hash;
        }
    }

    /**
     * Persisted Type Edge that connects two Type Vertices, and an overridden Type Vertex
     */
    public static class Persisted extends TypeEdgeImpl implements TypeEdge {

        private final VertexIID.Type fromIID;
        private final VertexIID.Type toIID;
        private final Encoding.Edge.Type encoding;
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
        public Persisted(TypeGraph graph, EdgeViewIID.Type iid, @Nullable VertexIID.Type overriddenIID) {
            super(graph, iid.encoding());

            if (iid.isForward()) {
                fromIID = iid.start();
                toIID = iid.end();
            } else {
                fromIID = iid.end();
                toIID = iid.start();
            }
            encoding = iid.encoding();
            deleted = new AtomicBoolean(false);
            if (overriddenIID != null) this.overriddenIID = overriddenIID;
        }

        @Override
        public Encoding.Edge.Type encoding() {
            return encoding;
        }

        @Override
        public EdgeViewIID.Type computeForwardIID() {
            return EdgeViewIID.Type.of(fromIID, encoding.forward(), toIID);
        }

        public EdgeViewIID.Type computeBackwardIID() {
            return EdgeViewIID.Type.of(toIID, encoding.backward(), fromIID);
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
        public Optional<TypeVertex> overridden() {
            if (overridden != null) return Optional.of(overridden);
            if (overriddenIID == null) return Optional.empty();
            overridden = graph.convert(overriddenIID);
            return Optional.of(overridden);
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
            graph.storage().putUntracked(computeForwardIID(), overriddenIID.bytes());
            graph.storage().putUntracked(computeBackwardIID(), overriddenIID.bytes());
        }

        /**
         * Delete operation of a persisted edge.
         *
         * This operation can only be performed once, and thus protected by
         * {@code isDelete} atomic boolean. We mark both from and to vertices
         * as modified, and delete both directions of this edge from the graph storage.
         */
        @Override
        public void delete() {
            if (deleted.compareAndSet(false, true)) {
                from().outs().remove(this);
                to().ins().remove(this);
                graph.storage().deleteUntracked(forwardView().iid());
                graph.storage().deleteUntracked(backwardView().iid());
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
        public void commit() {
        }

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
