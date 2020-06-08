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

package hypergraph.graph.iid;

import hypergraph.graph.util.Schema;

import static hypergraph.common.collection.Bytes.join;
import static java.util.Arrays.copyOfRange;

public abstract class EdgeIID<
        EDGE_SCHEMA extends Schema.Edge,
        VERTEX_IID_START extends VertexIID,
        VERTEX_IID_END extends VertexIID> extends IID {

    EdgeIID(byte[] bytes) {
        super(bytes);
    }

    public abstract boolean isOutwards();

    public abstract EDGE_SCHEMA schema();

    public abstract VERTEX_IID_START start();

    public abstract VERTEX_IID_END end();

    @Override
    public String toString() {
        return "[" + VertexIID.Type.LENGTH + ": " + start().toString() + "]" +
                "[" + InfixIID.LENGTH + ": " + schema().toString() + "]" +
                "[" + VertexIID.Type.LENGTH + ": " + end().toString() + "]";
    }

    public static class Type extends EdgeIID<Schema.Edge.Type, VertexIID.Type, VertexIID.Type> {

        private VertexIID.Type start;
        private VertexIID.Type end;

        Type(byte[] bytes) {
            super(bytes);
        }

        public static Type of(byte[] bytes) {
            return new Type(bytes);
        }

        public static Type of(VertexIID.Type start, Schema.Infix infix, VertexIID.Type end) {
            return new Type(join(start.bytes, infix.bytes(), end.bytes));
        }

        @Override
        public boolean isOutwards() {
            return Schema.Edge.isOut(bytes[VertexIID.Type.LENGTH]);
        }

        @Override
        public Schema.Edge.Type schema() {
            return Schema.Edge.Type.of(bytes[VertexIID.Type.LENGTH]);
        }

        @Override
        public VertexIID.Type start() {
            if (start != null) return start;
            start = VertexIID.Type.of(copyOfRange(bytes, 0, VertexIID.Type.LENGTH));
            return start;
        }

        @Override
        public VertexIID.Type end() {
            if (end != null) return end;
            end = VertexIID.Type.of(copyOfRange(bytes, bytes.length - VertexIID.Type.LENGTH, bytes.length));
            return end;
        }
    }

    public static class Thing extends EdgeIID<Schema.Edge.Thing, VertexIID.Thing, VertexIID.Thing> {

        Thing(byte[] bytes) {
            super(bytes);
        }

        public static Thing of(byte[] bytes) {
            return new Thing(bytes);
        }

        public static Thing of(VertexIID.Thing start, Schema.Infix infix, VertexIID.Thing end) {
            return new Thing(join(start.bytes(), infix.bytes(), end.bytes()));
        }

        @Override
        public boolean isOutwards() {
            return false; // TODO
        }

        @Override
        public Schema.Edge.Thing schema() {
            return null; // TODO
        }

        @Override
        public VertexIID.Thing start() {
            return null; // TODO
        }

        @Override
        public VertexIID.Thing end() {
            return null; // TODO
        }
    }

    public static class InwardsISA extends EdgeIID<Schema.Edge.Thing, VertexIID.Type, VertexIID.Thing> {

        private VertexIID.Type start;
        private VertexIID.Thing end;

        InwardsISA(byte[] bytes) {
            super(bytes);
        }

        public static InwardsISA of(byte[] bytes) {
            return new InwardsISA(bytes);
        }

        public static InwardsISA of(VertexIID.Type start, VertexIID.Thing end) {
            return new InwardsISA(join(start.bytes, Schema.Edge.Thing.ISA.in().bytes(), end.bytes));
        }

        @Override
        public boolean isOutwards() {
            return false;
        }

        @Override
        public Schema.Edge.Thing schema() {
            return Schema.Edge.Thing.ISA;
        }

        @Override
        public VertexIID.Type start() {
            if (start != null) return start;
            start = VertexIID.Type.of(copyOfRange(bytes, 0, VertexIID.Type.LENGTH));
            return start;
        }

        @Override
        public VertexIID.Thing end() {
            if (end != null) return end;
            end = VertexIID.Thing.of(copyOfRange(bytes, VertexIID.Type.LENGTH + 1, bytes.length));
            return end;
        }
    }
}
