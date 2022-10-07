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

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.graph.ThingGraph;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.encoding.iid.EdgeViewIID;
import com.vaticle.typedb.core.encoding.iid.InfixIID;
import com.vaticle.typedb.core.encoding.iid.KeyIID;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.vaticle.typedb.core.common.collection.ByteArray.join;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.ILLEGAL_OPERATION;
import static com.vaticle.typedb.core.encoding.Encoding.Prefix.VERTEX_ROLE;
import static com.vaticle.typedb.core.encoding.Encoding.Status.PERSISTED;
import static java.util.Objects.hash;

public abstract class ThingEdgeImpl implements ThingEdge {

    final ThingGraph graph;
    final Encoding.Edge.Thing encoding;
    final View.Forward forward;
    final View.Backward backward;
    final AtomicBoolean deleted;
    final boolean isInferred;

    ThingEdgeImpl(ThingGraph graph, Encoding.Edge.Thing encoding, boolean isInferred) {
        this.graph = graph;
        this.encoding = encoding;
        this.deleted = new AtomicBoolean(false);
        this.isInferred = isInferred;
        this.forward = new View.Forward(this);
        this.backward = new View.Backward(this);
    }

    @Override
    public boolean isInferred() {
        return isInferred;
    }

    @Override
    public View.Forward forwardView() {
        return forward;
    }

    @Override
    public View.Backward backwardView() {
        return backward;
    }

    abstract EdgeViewIID.Thing computeForwardIID();

    abstract EdgeViewIID.Thing computeBackwardIID();

    private static abstract class View<T extends ThingEdge.View<T>> implements ThingEdge.View<T> {

        final ThingEdgeImpl edge;
        EdgeViewIID.Thing iidCache = null;

        private View(ThingEdgeImpl edge) {
            this.edge = edge;
        }

        @Override
        public ThingEdge edge() {
            return edge;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || this.getClass() != object.getClass()) return false;
            return edge.equals(((ThingEdgeImpl.View<?>) object).edge);
        }

        @Override
        public int hashCode() {
            return edge.hashCode();
        }

        private static class Forward extends ThingEdgeImpl.View<ThingEdge.View.Forward> implements ThingEdge.View.Forward {

            private Forward(ThingEdgeImpl edge) {
                super(edge);
            }

            @Override
            public EdgeViewIID.Thing iid() {
                if (iidCache == null) iidCache = edge.computeForwardIID();
                return iidCache;
            }

            @Override
            public int compareTo(ThingEdge.View.Forward other) {
                return iid().compareTo(other.iid());
            }
        }

        private static class Backward extends ThingEdgeImpl.View<ThingEdge.View.Backward> implements ThingEdge.View.Backward {

            private Backward(ThingEdgeImpl edge) {
                super(edge);
            }

            @Override
            public EdgeViewIID.Thing iid() {
                if (iidCache == null) iidCache = edge.computeBackwardIID();
                return iidCache;
            }

            @Override
            public int compareTo(ThingEdge.View.Backward other) {
                return iid().compareTo(other.iid());
            }
        }
    }

    public static class Buffered extends ThingEdgeImpl implements ThingEdge {

        private final AtomicBoolean committed;
        private final ThingVertex.Write from;
        private final ThingVertex.Write to;
        private final ThingVertex.Write optimised;
        private final int hash;

        /**
         * Default constructor for {@code ThingEdgeImpl.Buffered}.
         *
         * @param encoding   the edge {@code Encoding}
         * @param from       the tail vertex
         * @param to         the head vertex
         * @param isInferred
         */
        public Buffered(Encoding.Edge.Thing encoding, ThingVertex.Write from, ThingVertex.Write to, boolean isInferred) {
            this(encoding, from, to, null, isInferred);
        }

        /**
         * Constructor for an optimised {@code ThingEdgeImpl.Buffered}.
         *
         * @param encoding  the edge {@code Encoding}
         * @param from      the tail vertex
         * @param to        the head vertex
         * @param optimised vertex that this optimised edge is compressing
         */
        public Buffered(Encoding.Edge.Thing encoding, ThingVertex.Write from, ThingVertex.Write to,
                        @Nullable ThingVertex.Write optimised, boolean isInferred) {
            super(from.graph(), encoding, isInferred);
            assert this.graph == to.graph();
            assert encoding.isOptimisation() || optimised == null;
            this.from = from;
            this.to = to;
            this.optimised = optimised;
            this.hash = hash(Buffered.class, encoding, from, to);
            committed = new AtomicBoolean(false);
        }

        @Override
        public Encoding.Edge.Thing encoding() {
            return encoding;
        }

        @Override
        public ThingVertex.Write from() {
            return from;
        }

        @Override
        public VertexIID.Thing fromIID() {
            return from.iid();
        }

        @Override
        public ThingVertex.Write to() {
            return to;
        }

        @Override
        public VertexIID.Thing toIID() {
            return to.iid();
        }

        @Override
        public Optional<ThingVertex> optimised() {
            return Optional.ofNullable(optimised);
        }

        @Override
        EdgeViewIID.Thing computeForwardIID() {
            if (encoding().isOptimisation()) {
                return EdgeViewIID.Thing.of(
                        fromIID(), InfixIID.Thing.of(encoding().forward(), optimised().get().type().iid()),
                        toIID(), optimised().get().iid().key()
                );
            } else {
                return EdgeViewIID.Thing.of(fromIID(), InfixIID.Thing.of(encoding().forward()), toIID());
            }
        }

        @Override
        EdgeViewIID.Thing computeBackwardIID() {
            if (encoding().isOptimisation()) {
                return EdgeViewIID.Thing.of(
                        toIID(), InfixIID.Thing.of(encoding().backward(), optimised().get().type().iid()),
                        fromIID(), optimised().get().iid().key()
                );
            } else {
                return EdgeViewIID.Thing.of(toIID(), InfixIID.Thing.of(encoding().backward()), fromIID());
            }
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
                from.outs().remove(this);
                to.ins().remove(this);
                if (from.status().equals(PERSISTED) && to.status().equals(PERSISTED)) {
                    graph.storage().deleteTracked(forward.iid());
                    graph.storage().deleteUntracked(backward.iid());
                }
                graph.edgeDeleted(this);
            }
        }

        @Override
        public void commit() {
            if (isInferred()) throw TypeDBException.of(ILLEGAL_OPERATION);
            if (committed.compareAndSet(false, true)) {
                graph.storage().putTracked(computeForwardIID()); // re-compute IID because vertices may be committed
                graph.storage().putUntracked(computeBackwardIID());
            }
        }

        /**
         * Determine the equality of a {@code ThingEdgeImpl.Buffered} against another.
         *
         * We only use {@code encoding}, {@code from} and {@code to} as the are
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
            return (this.encoding.equals(that.encoding) &&
                    this.from.equals(that.from) &&
                    this.to.equals(that.to) &&
                    Objects.equals(this.optimised, that.optimised));
        }

        /**
         * Determine the equality of a {@code Edge.Buffered} against another.
         *
         * We only use {@code encoding}, {@code from} and {@code to} as the are
         * the fixed properties that do not change, unlike {@code overridden}.
         * They are also the canonical properties required to uniquely identify
         * a {@code ThingEdgeImpl.Buffered}.
         *
         * @return int of the hashcode
         */
        @Override
        public final int hashCode() {
            return hash;
        }
    }

    public static class Target extends ThingEdgeImpl implements ThingEdge {

        private final ThingVertex from;
        private final ThingVertex to;
        private final TypeVertex optimisedType;
        private final int hash;

        public Target(Encoding.Edge.Thing encoding, ThingVertex from, ThingVertex to, @Nullable TypeVertex optimisedType) {
            super(from.graph(), encoding, false);
            assert !encoding.isOptimisation() || optimisedType != null;
            this.from = from;
            this.to = to;
            this.optimisedType = optimisedType;
            this.hash = hash(Target.class, encoding, from, to, optimisedType);
        }

        @Override
        public Encoding.Edge.Thing encoding() {
            return encoding;
        }

        @Override
        EdgeViewIID.Thing computeForwardIID() {
            if (encoding().isOptimisation()) {
                return EdgeViewIID.Thing.of(
                        fromIID(), InfixIID.Thing.of(encoding().forward(), optimisedType.iid()),
                        toIID(), KeyIID.of(ByteArray.empty())
                );
            } else {
                return EdgeViewIID.Thing.of(fromIID(), InfixIID.Thing.of(encoding().forward()), toIID());
            }
        }

        @Override
        EdgeViewIID.Thing computeBackwardIID() {
            if (encoding.isOptimisation()) {
                return EdgeViewIID.Thing.of(toIID(), InfixIID.Thing.of(encoding().backward(), optimisedType.iid()),
                        fromIID(), KeyIID.of(ByteArray.empty()));
            } else {
                return EdgeViewIID.Thing.of(toIID(), InfixIID.Thing.of(encoding().backward()), fromIID());
            }
        }

        @Override
        public ThingVertex from() {
            return from;
        }

        @Override
        public VertexIID.Thing fromIID() {
            return from.iid();
        }

        @Override
        public ThingVertex to() {
            return to;
        }

        @Override
        public VertexIID.Thing toIID() {
            return to.iid();
        }

        @Override
        public Optional<ThingVertex> optimised() {
            return Optional.empty();
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
            ThingEdgeImpl.Target that = (ThingEdgeImpl.Target) object;
            return this.encoding.equals(that.encoding) &&
                    this.from.equals(that.from) &&
                    this.to.equals(that.to) &&
                    Objects.equals(this.optimisedType, that.optimisedType);
        }

        @Override
        public final int hashCode() {
            return hash;
        }
    }

    public static class Persisted extends ThingEdgeImpl implements ThingEdge {

        private final VertexIID.Thing fromIID;
        private final VertexIID.Thing toIID;
        private final VertexIID.Thing optimisedIID;
        private final int hash;

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
        public Persisted(ThingGraph graph, EdgeViewIID.Thing iid) {
            super(graph, iid.encoding(), false);

            if (iid.isForward()) {
                fromIID = iid.start();
                toIID = iid.end();
            } else {
                fromIID = iid.end();
                toIID = iid.start();
            }
            if (!iid.suffix().isEmpty()) {
                optimisedIID = VertexIID.Thing.of(join(
                        VERTEX_ROLE.bytes(), iid.infix().asRolePlayer().tail().bytes(), iid.suffix().bytes()
                ));
            } else {
                optimisedIID = null;
            }

            this.hash = hash(Persisted.class, encoding, fromIID.hashCode(), toIID.hashCode());
        }

        @Override
        public Encoding.Edge.Thing encoding() {
            return encoding;
        }

        @Override
        public ThingVertex from() {
            // note: do not cache, since a readable vertex can become a writable vertex at any time
            return graph.convertToReadable(fromIID);
        }

        @Override
        public VertexIID.Thing fromIID() {
            return fromIID;
        }

        @Override
        public ThingVertex to() {
            return graph.convertToReadable(toIID);
        }

        @Override
        public VertexIID.Thing toIID() {
            return toIID;
        }

        @Override
        public Optional<ThingVertex> optimised() {
            return Optional.ofNullable(graph.convertToReadable(optimisedIID));
        }

        @Override
        EdgeViewIID.Thing computeForwardIID() {
            if (encoding().isOptimisation()) {
                return EdgeViewIID.Thing.of(
                        fromIID(), InfixIID.Thing.of(encoding().forward(), optimisedIID.type()),
                        toIID(), optimisedIID.key()
                );
            } else {
                return EdgeViewIID.Thing.of(fromIID(), InfixIID.Thing.of(encoding().forward()), toIID());
            }
        }

        @Override
        EdgeViewIID.Thing computeBackwardIID() {
            if (encoding().isOptimisation()) {
                return EdgeViewIID.Thing.of(
                        toIID(), InfixIID.Thing.of(encoding().backward(), optimisedIID.type()),
                        fromIID(), optimisedIID.key()
                );
            } else {
                return EdgeViewIID.Thing.of(toIID(), InfixIID.Thing.of(encoding().backward()), fromIID());
            }
        }

        /**
         * Delete operation of a persisted edge.
         *
         * This operation can only be performed once, and thus protected by
         * {@code isDelete} atomic boolean. The delete operation involves
         * removing this edge from the graph storage and notifying the from/to vertices of the modification.
         */
        @Override
        public void delete() {
            if (deleted.compareAndSet(false, true)) {
                graph.convertToWritable(fromIID).setModified();
                graph.convertToWritable(toIID).setModified();
                graph.storage().deleteTracked(forward.iid());
                graph.storage().deleteUntracked(backward.iid());
                graph.edgeDeleted(this);
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
            ThingEdgeImpl.Persisted that = (ThingEdgeImpl.Persisted) object;
            return (this.encoding.equals(that.encoding) &&
                    this.fromIID.equals(that.fromIID) &&
                    this.toIID.equals(that.toIID) &&
                    Objects.equals(this.optimisedIID, that.optimisedIID));
        }

        /**
         * HashCode of a {@code ThingEdgeImpl.Persisted}.
         *
         * We only use {@code encoding}, {@code fromIID} and {@code toIID} as the
         * are the fixed properties that do not change, unlike
         * {@code overriddenIID} and {@code isDeleted}. They are also the
         * canonical properties required to uniquely identify an
         * {@code ThingEdgeImpl.Persisted}.
         *
         * @return int of the hashcode
         */
        @Override
        public final int hashCode() {
            return hash;
        }
    }
}
