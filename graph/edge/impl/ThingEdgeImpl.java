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

package grakn.core.graph.edge.impl;

import grakn.core.graph.DataGraph;
import grakn.core.graph.edge.ThingEdge;
import grakn.core.graph.iid.EdgeIID;
import grakn.core.graph.iid.InfixIID;
import grakn.core.graph.iid.SuffixIID;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.ThingVertex;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static grakn.core.common.collection.Bytes.join;
import static grakn.core.graph.util.Encoding.Prefix.VERTEX_ROLE;
import static grakn.core.graph.util.Encoding.Status.BUFFERED;
import static java.util.Objects.hash;

public abstract class ThingEdgeImpl implements ThingEdge {

    final DataGraph graph;
    final Encoding.Edge.Thing encoding;
    final AtomicBoolean deleted;

    ThingEdgeImpl(DataGraph graph, Encoding.Edge.Thing encoding) {
        this.graph = graph;
        this.encoding = encoding;
        deleted = new AtomicBoolean(false);
    }

    public static class Buffered extends ThingEdgeImpl implements ThingEdge {

        private final AtomicBoolean committed;
        private final ThingVertex from;
        private final ThingVertex to;
        private final ThingVertex optimised;
        private final int hash;

        /**
         * Default constructor for {@code ThingEdgeImpl.Buffered}.
         *
         * @param encoding the edge {@code Encoding}
         * @param from     the tail vertex
         * @param to       the head vertex
         */
        public Buffered(Encoding.Edge.Thing encoding, ThingVertex from, ThingVertex to) {
            this(encoding, from, to, null);
        }

        /**
         * Constructor for an optimised {@code ThingEdgeImpl.Buffered}.
         *
         * @param encoding  the edge {@code Encoding}
         * @param from      the tail vertex
         * @param to        the head vertex
         * @param optimised vertex that this optimised edge is compressing
         */
        public Buffered(Encoding.Edge.Thing encoding, ThingVertex from, ThingVertex to, @Nullable ThingVertex optimised) {
            super(from.graph(), encoding);
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
        public EdgeIID.Thing outIID() {
            if (encoding.isOptimisation()) {
                return EdgeIID.Thing.of(from.iid(), InfixIID.Thing.of(encoding.out(), optimised.type().iid()),
                                        to.iid(), SuffixIID.of(optimised.iid().key()));
            } else {
                return EdgeIID.Thing.of(from.iid(), InfixIID.Thing.of(encoding.out()), to.iid());
            }
        }

        @Override
        public EdgeIID.Thing inIID() {
            if (encoding.isOptimisation()) {
                return EdgeIID.Thing.of(to.iid(), InfixIID.Thing.of(encoding.in(), optimised.type().iid()),
                                        from.iid(), SuffixIID.of(optimised.iid().key()));
            } else {
                return EdgeIID.Thing.of(to.iid(), InfixIID.Thing.of(encoding.in()), from.iid());
            }
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
        public Optional<ThingVertex> optimised() {
            return Optional.ofNullable(optimised);
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
                if (!(from.status().equals(BUFFERED)) && !(to.status().equals(BUFFERED))) {
                    graph.storage().delete(outIID().bytes());
                    graph.storage().delete(inIID().bytes());
                }
                if (encoding == Encoding.Edge.Thing.HAS) {
                    graph.stats().hasEdgeDeleted(from.iid(), to.iid().asAttribute());
                }
            }
        }

        @Override
        public void commit() {
            if (committed.compareAndSet(false, true)) {
                graph.storage().put(outIID().bytes());
                graph.storage().put(inIID().bytes());
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
            final ThingEdgeImpl.Buffered that = (ThingEdgeImpl.Buffered) object;
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

    public static class Persisted extends ThingEdgeImpl implements ThingEdge {

        private final EdgeIID.Thing outIID;
        private final EdgeIID.Thing inIID;
        private final VertexIID.Thing fromIID;
        private final VertexIID.Thing toIID;
        private final VertexIID.Thing optimisedIID;
        private final int hash;
        private ThingVertex from;
        private ThingVertex to;
        private ThingVertex optimised;

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
        public Persisted(DataGraph graph, EdgeIID.Thing iid) {
            super(graph, iid.encoding());

            if (iid.isOutwards()) {
                fromIID = iid.start();
                toIID = iid.end();
                outIID = iid;
                inIID = EdgeIID.Thing.of(iid.end(), iid.infix().inwards(), iid.start(), iid.suffix());
            } else {
                fromIID = iid.end();
                toIID = iid.start();
                inIID = iid;
                outIID = EdgeIID.Thing.of(iid.end(), iid.infix().outwards(), iid.start(), iid.suffix());
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
        public EdgeIID.Thing outIID() {
            return outIID;
        }

        @Override
        public EdgeIID.Thing inIID() {
            return inIID;
        }

        @Override
        public ThingVertex from() {
            if (from != null) return from;
            from = graph.convert(fromIID);
            from.outs().cache(this);
            return from;
        }

        @Override
        public ThingVertex to() {
            if (to != null) return to;
            to = graph.convert(toIID);
            to.ins().cache(this);
            return to;
        }

        @Override
        public Optional<ThingVertex> optimised() {
            if (optimised != null) return Optional.of(optimised);
            if (optimisedIID != null) optimised = graph.convert(optimisedIID);
            return Optional.ofNullable(optimised);
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
                graph.storage().delete(this.outIID.bytes());
                graph.storage().delete(this.inIID.bytes());
                if (encoding == Encoding.Edge.Thing.HAS) {
                    graph.stats().hasEdgeDeleted(fromIID, toIID.asAttribute());
                }
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
            final ThingEdgeImpl.Persisted that = (ThingEdgeImpl.Persisted) object;
            return (this.encoding.equals(that.encoding) &&
                    this.fromIID.equals(that.fromIID) &&
                    this.toIID.equals(that.toIID));
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
