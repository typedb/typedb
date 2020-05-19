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

package hypergraph.graph.util;

import javax.annotation.Nullable;
import java.util.Arrays;

import static hypergraph.common.collection.ByteArrays.join;
import static java.util.Arrays.copyOfRange;

public class IID {

    protected final byte[] bytes;

    IID(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] bytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return Arrays.toString(bytes);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        IID that = (IID) object;
        return Arrays.equals(this.bytes, that.bytes);
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(bytes);
    }

    public static class Prefix extends IID {

        public static final int LENGTH = 1;

        Prefix(byte[] bytes) {
            super(bytes);
            assert bytes.length == LENGTH;
        }

        public static Prefix of(byte[] bytes) {
            return new Prefix(bytes);
        }
    }

    public static class Infix extends IID {

        public static final int LENGTH = 1;

        Infix(byte[] bytes) {
            super(bytes);
            assert bytes.length == LENGTH;
        }

        public static Infix of(byte[] bytes) {
            return new Infix(bytes);
        }
    }

    public static abstract class Index extends IID {

        Index(byte[] bytes) {
            super(bytes);
        }

        public static class Type extends Index {

            Type(byte[] bytes) {
                super(bytes);
            }

            /**
             * Returns the index address of given {@code TypeVertex}
             *
             * @param label of the {@code TypeVertex}
             * @param scope of the {@code TypeVertex}, which could be null
             * @return a byte array representing the index address of a {@code TypeVertex}
             */
            public static Index of(String label, @Nullable String scope) {
                return new Index.Type(join(Schema.Index.TYPE.prefix().bytes(), Schema.Vertex.Type.scopedLabel(label, scope).getBytes()));
            }
        }

    }

    public static abstract class Vertex extends IID {

        Vertex(byte[] bytes) {
            super(bytes);
        }

        public static class Type extends IID.Vertex {

            public static final int LENGTH = Prefix.LENGTH + 2;

            Type(byte[] bytes) {
                super(bytes);
                assert bytes.length == LENGTH;
            }

            public static IID.Vertex.Type of(byte[] bytes) {
                return new IID.Vertex.Type(bytes);
            }

            /**
             * Generate an IID for a {@code TypeVertex} for a given {@code Schema}
             *
             * @param keyGenerator to generate the IID for a {@code TypeVertex}
             * @param schema       of the {@code TypeVertex} in which the IID will be used for
             * @return a byte array representing a new IID for a {@code TypeVertex}
             */
            public static Type generate(KeyGenerator keyGenerator, Schema.Vertex.Type schema) {
                return of(join(schema.prefix().bytes(), keyGenerator.forType(Prefix.of(schema.prefix().bytes()))));
            }

            public Schema.Vertex.Type schema() {
                return Schema.Vertex.Type.of(bytes[0]);
            }
        }

        public static class Thing extends IID.Vertex {

            public static final int PREFIX_TYPE_LENGTH = Prefix.LENGTH + Type.LENGTH;
            public static final int LENGTH = PREFIX_TYPE_LENGTH + 8;

            Thing(byte[] bytes) {
                super(bytes);
            }

            public static IID.Vertex.Thing of(byte[] bytes) {
                return new IID.Vertex.Thing(bytes);
            }

            public IID.Vertex.Type type() {
                return IID.Vertex.Type.of(copyOfRange(bytes, Prefix.LENGTH, Type.LENGTH));
            }
        }

        public static class Attribute extends IID.Vertex.Thing {

            Attribute(byte[] bytes) {
                super(bytes);
            }
        }
    }

    public static abstract class Edge<EDGE_SCHEMA extends Schema.Edge, VERTEX_IID extends IID.Vertex> extends IID {

        Edge(byte[] bytes) {
            super(bytes);
        }

        public abstract boolean isOutwards();

        public abstract EDGE_SCHEMA schema();

        public abstract VERTEX_IID start();

        public abstract VERTEX_IID end();

        public static class Type extends IID.Edge<Schema.Edge.Type, IID.Vertex.Type> {

            Type(byte[] bytes) {
                super(bytes);
            }

            public static IID.Edge.Type of(byte[] bytes) {
                return new IID.Edge.Type(bytes);
            }

            public static IID.Edge.Type of(IID.Vertex.Type start, Schema.Infix infix, IID.Vertex.Type end) {
                return new IID.Edge.Type(join(start.bytes, infix.bytes(), end.bytes));
            }

            @Override
            public boolean isOutwards() {
                return Schema.Edge.isOut(bytes[Vertex.Type.LENGTH]);
            }

            @Override

            public Schema.Edge.Type schema() {
                return Schema.Edge.Type.of(bytes[Vertex.Type.LENGTH]);
            }

            @Override
            public Vertex.Type start() {
                return Vertex.Type.of(copyOfRange(bytes, 0, Vertex.Type.LENGTH));
            }

            @Override
            public Vertex.Type end() {
                return Vertex.Type.of(copyOfRange(bytes, Vertex.Type.LENGTH + Infix.LENGTH, bytes.length));
            }

        }

        public static class Thing extends IID.Edge<Schema.Edge.Thing, IID.Vertex.Thing> {

            Thing(byte[] bytes) {
                super(bytes);
            }

            public static IID.Edge.Thing of(byte[] bytes) {
                return new IID.Edge.Thing(bytes);
            }

            public static Thing of(Vertex.Thing start, Schema.Infix infix, Vertex.Thing end) {
                return new IID.Edge.Thing(join(start.bytes(), infix.bytes(), end.bytes()));
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
            public IID.Vertex.Thing start() {
                return null; // TODO
            }

            @Override
            public IID.Vertex.Thing end() {
                return null; // TODO
            }
        }
    }
}
