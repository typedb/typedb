package hypergraph.graph.iid;

import hypergraph.graph.util.Schema;

import java.util.Arrays;

import static hypergraph.common.collection.Bytes.join;
import static java.util.Arrays.copyOfRange;

public abstract class EdgeIID<EDGE_SCHEMA extends Schema.Edge, VERTEX_IID extends VertexIID> extends IID {

    EdgeIID(byte[] bytes) {
        super(bytes);
    }

    public abstract boolean isOutwards();

    public abstract EDGE_SCHEMA schema();

    public abstract VERTEX_IID start();

    public abstract VERTEX_IID end();

    public static class Type extends EdgeIID<Schema.Edge.Type, VertexIID.Type> {

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

        @Override
        public String toString() {
            start();
            return "[" + VertexIID.Type.LENGTH + ": " + start().toString() + "]" +
                    "[" + InfixIID.LENGTH + ": " + schema().toString() + "]" +
                    "[" + VertexIID.Type.LENGTH + ": " + end().toString() + "]";
        }
    }

    public static class Thing extends EdgeIID<Schema.Edge.Thing, VertexIID.Thing> {

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

        @Override
        public String toString() {
            return Arrays.toString(bytes); // TODO
        }
    }
}
