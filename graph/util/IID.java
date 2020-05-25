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
import java.time.LocalDateTime;
import java.util.Arrays;

import static hypergraph.common.collection.Bytes.*;
import static hypergraph.common.collection.Bytes.booleanToByte;
import static hypergraph.common.collection.Bytes.byteToBoolean;
import static hypergraph.common.collection.Bytes.bytesToDouble;
import static hypergraph.common.collection.Bytes.bytesToLong;
import static hypergraph.common.collection.Bytes.doubleToBytes;
import static hypergraph.common.collection.Bytes.join;
import static hypergraph.common.collection.Bytes.longToBytes;
import static hypergraph.common.collection.Bytes.stringToBytes;
import static hypergraph.graph.util.Schema.STRING_ENCODING;
import static hypergraph.graph.util.Schema.STRING_MAX_LENGTH;
import static hypergraph.graph.util.Schema.TIME_ZONE_ID;
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
            public static Index.Type of(String label, @Nullable String scope) {
                return new Index.Type(join(Schema.Index.TYPE.prefix().bytes(), Schema.Vertex.Type.scopedLabel(label, scope).getBytes()));
            }
        }

        public static class Attribute extends Index {

            Attribute(byte[] bytes) {
                super(bytes);
            }

            private static Index.Attribute newAttributeIndex(byte[] valueType, byte[] value, byte[] typeIID) {
                return new Index.Attribute(join(Schema.Index.ATTRIBUTE.prefix().bytes(), valueType, value, typeIID));
            }

            public static Index.Attribute of(boolean value, IID.Vertex.Attribute attributeIID) {
                return newAttributeIndex(Schema.ValueType.BOOLEAN.bytes(), new byte[]{booleanToByte(value)}, attributeIID.type().bytes);
            }

            public static Index.Attribute of(long value, IID.Vertex.Attribute attributeIID) {
                return newAttributeIndex(Schema.ValueType.LONG.bytes(), longToBytes(value), attributeIID.type().bytes);
            }

            public static Index.Attribute of(double value, IID.Vertex.Attribute attributeIID) {
                return newAttributeIndex(Schema.ValueType.DOUBLE.bytes(), doubleToBytes(value), attributeIID.type().bytes);
            }

            public static Index.Attribute of(String value, IID.Vertex.Attribute attributeIID) {
                return newAttributeIndex(Schema.ValueType.STRING.bytes(), stringToBytes(value, STRING_ENCODING), attributeIID.type().bytes);
            }

            public static Index.Attribute of(LocalDateTime value, IID.Vertex.Attribute attributeIID) {
                return newAttributeIndex(Schema.ValueType.DATETIME.bytes(), dateTimeToBytes(value, TIME_ZONE_ID), attributeIID.type().bytes);
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

            public Thing(byte[] bytes) {
                super(bytes);
            }

            /**
             * Generate an IID for a {@code ThingVertex} for a given {@code Schema} and {@code TypeVertex}
             *
             * @param keyGenerator to generate the IID for a {@code ThingVertex}
             * @param typeIID      of the {@code TypeVertex} in which this {@code ThingVertex} is an instance of
             * @return a byte array representing a new IID for a {@code ThingVertex}
             */
            public static Thing generate(KeyGenerator keyGenerator, Type typeIID) {
                return new Thing(join(Schema.Vertex.Thing.of(typeIID.schema()).prefix().bytes(),
                                      typeIID.bytes(),
                                      keyGenerator.forThing(typeIID)));
            }

            public IID.Vertex.Type type() {
                return IID.Vertex.Type.of(copyOfRange(bytes, Prefix.LENGTH, Type.LENGTH));
            }

            public Schema.Vertex.Thing schema() {
                return Schema.Vertex.Thing.of(bytes[0]);
            }
        }

        public static abstract class Attribute<VALUE> extends IID.Vertex.Thing {

            public static final int VALUE_INDEX = Prefix.LENGTH + Type.LENGTH + 1;

            Attribute(Schema.ValueType valueType, Type typeIID, byte[] valueBytes) {
                super(join(
                        Schema.Vertex.Thing.ATTRIBUTE.prefix().bytes(),
                        typeIID.bytes(),
                        valueType.bytes(),
                        valueBytes
                ));
            }

            public abstract VALUE value();

            public static class Boolean extends Attribute<java.lang.Boolean> {

                public Boolean(IID.Vertex.Type typeIID, boolean value) {
                    super(Schema.ValueType.BOOLEAN, typeIID, new byte[]{booleanToByte(value)});
                }

                @Override
                public java.lang.Boolean value() {
                    return byteToBoolean(bytes[VALUE_INDEX]);
                }
            }

            public static class Long extends Attribute<java.lang.Long> {

                public Long(IID.Vertex.Type typeIID, long value) {
                    super(Schema.ValueType.LONG, typeIID, longToBytes(value));
                }

                @Override
                public java.lang.Long value() {
                    return bytesToLong(copyOfRange(bytes, VALUE_INDEX, VALUE_INDEX + LONG_SIZE));
                }
            }

            public static class Double extends Attribute<java.lang.Double> {

                public Double(IID.Vertex.Type typeIID, double value) {
                    super(Schema.ValueType.DOUBLE, typeIID, doubleToBytes(value));
                }

                @Override
                public java.lang.Double value() {
                    return bytesToDouble(copyOfRange(bytes, VALUE_INDEX, VALUE_INDEX + DOUBLE_SIZE));
                }
            }

            public static class String extends Attribute<java.lang.String> {

                public String(IID.Vertex.Type typeIID, java.lang.String value) {
                    super(Schema.ValueType.STRING, typeIID, stringToBytes(value, STRING_ENCODING));
                    assert bytes.length <= STRING_MAX_LENGTH + 1;
                }

                @Override
                public java.lang.String value() {
                    return bytesToString(copyOfRange(bytes, VALUE_INDEX, bytes.length), STRING_ENCODING);
                }
            }

            public static class DateTime extends Attribute<java.time.LocalDateTime> {

                public DateTime(IID.Vertex.Type typeIID, java.time.LocalDateTime value) {
                    super(Schema.ValueType.DATETIME, typeIID, dateTimeToBytes(value, TIME_ZONE_ID));
                }

                @Override
                public java.time.LocalDateTime value() {
                    return bytesToDateTime(copyOfRange(bytes, VALUE_INDEX, bytes.length), TIME_ZONE_ID);
                }
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
