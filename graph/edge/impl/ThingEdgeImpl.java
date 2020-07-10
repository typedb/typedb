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

import grakn.graph.ThingGraph;
import grakn.graph.edge.ThingEdge;
import grakn.graph.iid.EdgeIID;
import grakn.graph.iid.InfixIID;
import grakn.graph.iid.SuffixIID;
import grakn.graph.iid.VertexIID;
import grakn.graph.util.Schema;
import grakn.graph.vertex.ThingVertex;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static grakn.graph.util.Schema.Status.BUFFERED;
import static java.util.Objects.hash;

public abstract class ThingEdgeImpl implements ThingEdge {

    final ThingGraph graph;
    final Schema.Edge.Thing schema;
    final AtomicBoolean deleted;

    ThingEdgeImpl(ThingGraph graph, Schema.Edge.Thing schema) {
        this.graph = graph;
        this.schema = schema;
        deleted = new AtomicBoolean(false);
    }

    public static class Buffered extends ThingEdgeImpl implements ThingEdge {

        private final AtomicBoolean committed;
        private final ThingVertex from;
        private final ThingVertex to;
        private ThingVertex optimised;
        private int hash;

        /**
         * Default constructor for {@code ThingEdgeImpl.Buffered}.
         *
         * @param schema the edge {@code Schema}
         * @param from   the tail vertex
         * @param to     the head vertex
         */
        public Buffered(Schema.Edge.Thing schema, ThingVertex from, ThingVertex to) {
            super(from.graph(), schema);
            assert this.graph == to.graph();
            this.from = from;
            this.to = to;
            committed = new AtomicBoolean(false);
        }

        /**
         * Constructor for an optimised {@code ThingEdgeImpl.Buffered}.
         *
         * @param schema    the edge {@code Schema}
         * @param from      the tail vertex
         * @param to        the head vertex
         * @param optimised vertex that this optimised edge is compressing
         */
        public Buffered(Schema.Edge.Thing schema, ThingVertex from, ThingVertex to, ThingVertex optimised) {
            this(schema, from, to);
            assert schema.isOptimisation() && optimised != null;
            this.optimised = optimised;
        }

        @Override
        public Schema.Edge.Thing schema() {
            return schema;
        }

        @Override
        public EdgeIID.Thing outIID() {
            if (schema.isOptimisation()) {
                return EdgeIID.Thing.of(from.iid(), InfixIID.Thing.of(schema.out(), optimised.type().iid()),
                                        to.iid(), SuffixIID.of(optimised.iid().key()));
            } else {
                return EdgeIID.Thing.of(from.iid(), InfixIID.Thing.of(schema.out()), to.iid());
            }
        }

        @Override
        public EdgeIID.Thing inIID() {
            if (schema.isOptimisation()) {
                return EdgeIID.Thing.of(to.iid(), InfixIID.Thing.of(schema.in(), optimised.type().iid()),
                                        from.iid(), SuffixIID.of(optimised.iid().key()));
            } else {
                return EdgeIID.Thing.of(to.iid(), InfixIID.Thing.of(schema.in()), from.iid());
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

        /**
         * Deletes this {@code Edge} from connecting between two {@code Vertex}.
         *
         * A {@code ThingEdgeImpl.Buffered} can only exist in the adjacency cache of
         * each {@code Vertex}, and does not exist in storage.
         */
        @Override
        public void delete() {
            if (deleted.compareAndSet(false, true)) {
                from.outs().removeFromBuffer(this);
                to.ins().removeFromBuffer(this);
                if (!(from.status().equals(BUFFERED)) && !(to.status().equals(BUFFERED))) {
                    graph.storage().delete(outIID().bytes());
                    graph.storage().delete(inIID().bytes());
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
                    this.to.equals(that.to) &&
                    Objects.equals(this.optimised, that.optimised));
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
            if (hash == 0) hash = hash(schema, from, to, optimised);
            return hash;
        }
    }

    public static class Persisted extends ThingEdgeImpl implements ThingEdge {

        private final EdgeIID.Thing outIID;
        private final EdgeIID.Thing inIID;
        private final VertexIID.Thing fromIID;
        private final VertexIID.Thing toIID;
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
            super(graph, iid.schema());

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
        }

        @Override
        public Schema.Edge.Thing schema() {
            return schema;
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
            from.outs().loadToBuffer(this);
            return from;
        }

        @Override
        public ThingVertex to() {
            if (to != null) return to;
            to = graph.convert(toIID);
            to.ins().loadToBuffer(this);
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
