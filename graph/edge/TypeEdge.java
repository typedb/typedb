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

package hypergraph.graph.edge;

import hypergraph.graph.Graph;
import hypergraph.graph.Schema;
import hypergraph.graph.vertex.TypeVertex;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static hypergraph.graph.util.ByteArrays.join;
import static java.util.Arrays.copyOfRange;

public abstract class TypeEdge extends Edge<Schema.Edge.Type, TypeVertex> {

    protected final Graph.Type graph;

    TypeEdge(Graph.Type graph, Schema.Edge.Type schema) {
        super(schema);
        this.graph = graph;
    }

    public static class Buffered extends TypeEdge {

        private AtomicBoolean committed;
        private final TypeVertex from;
        private final TypeVertex to;

        public Buffered(Graph.Type graph, Schema.Edge.Type schema, TypeVertex from, TypeVertex to) {
            super(graph, schema);
            this.from = from;
            this.to = to;
            committed = new AtomicBoolean(false);
        }

        @Override
        public Schema.Status status() {
            return committed.get() ? Schema.Status.COMMITTED : Schema.Status.BUFFERED;
        }

        @Override
        public byte[] outIID() {
            return join(from().iid(), schema.out().key(), to().iid());
        }

        @Override
        public byte[] inIID() {
            return join(to().iid(), schema.in().key(), from().iid());
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
        public void delete() {
            from.outs().removeNonRecursive(this);
            to.ins().removeNonRecursive(this);
        }

        @Override
        public void commit() {
            if (committed.compareAndSet(false, true)) {
                if (schema.out() != null) {
                    graph.storage().put(outIID());
                }
                if (schema.in() != null) {
                    graph.storage().put(inIID());
                }
            }
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            TypeEdge.Buffered that = (TypeEdge.Buffered) object;
            return (this.schema.equals(that.schema) &&
                    this.from.equals(that.from) &&
                    this.to.equals(that.to));
        }

        @Override
        public final int hashCode() {
            return Objects.hash(schema, from, to);
        }
    }

    public static class Persisted extends TypeEdge {

        private final byte[] outIID;
        private final byte[] inIID;
        private final byte[] fromIID;
        private final byte[] toIID;
        private final AtomicBoolean isDeleted;

        private TypeVertex from;
        private TypeVertex to;

        public Persisted(Graph.Type graph, byte[] iid) {
            super(graph, Schema.Edge.Type.of(iid[Schema.IID.TYPE.length()]));
            byte[] start = copyOfRange(iid, 0, Schema.IID.TYPE.length());
            byte[] end = copyOfRange(iid, Schema.IID.TYPE.length() + 1, iid.length);

            if (Schema.Edge.isOut(iid[Schema.IID.TYPE.length()])) {
                fromIID = start; toIID = end; outIID = iid;
                inIID = join(end, schema.in().key(), start);
            } else {
                fromIID = end; toIID = start; inIID = iid;
                outIID = join(end, schema().out().key(), start);
            }

            isDeleted = new AtomicBoolean(false);
        }

        @Override
        public Schema.Status status() {
            return Schema.Status.PERSISTED;
        }

        @Override
        public byte[] outIID() {
            return outIID;
        }

        @Override
        public byte[] inIID() {
            return inIID;
        }

        @Override
        public TypeVertex from() {
            if (from != null) return from;
            from = graph.getVertex(fromIID);
            return from;
        }

        @Override
        public TypeVertex to() {
            if (to != null) return to;
            to = graph.getVertex(toIID);
            return to;
        }

        @Override
        public void delete() {
            if (isDeleted.compareAndSet(false, true)) {
                if (from != null) from.outs().removeNonRecursive(this);
                if (to != null) to.ins().removeNonRecursive(this);
                graph.storage().delete(this.outIID);
                graph.storage().delete(this.inIID);
            }
        }

        @Override
        public void commit() {}

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            TypeEdge.Persisted that = (TypeEdge.Persisted) object;
            return (this.schema.equals(that.schema) &&
                    Arrays.equals(this.fromIID, that.fromIID) &&
                    Arrays.equals(this.toIID, that.toIID));
        }

        @Override
        public final int hashCode() {
            return Objects.hash(schema, Arrays.hashCode(fromIID), Arrays.hashCode(toIID));
        }
    }
}
