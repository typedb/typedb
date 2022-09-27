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

package com.vaticle.typedb.core.graph.iid;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBCheckedException;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.KeyGenerator;
import com.vaticle.typedb.core.graph.common.Storage.Key;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.collection.ByteArray.encodeDateTimeAsSorted;
import static com.vaticle.typedb.core.common.collection.ByteArray.encodeDoubleAsSorted;
import static com.vaticle.typedb.core.common.collection.ByteArray.encodeLongAsSorted;
import static com.vaticle.typedb.core.common.collection.ByteArray.encodeStringAsSorted;
import static com.vaticle.typedb.core.common.collection.ByteArray.join;
import static com.vaticle.typedb.core.common.collection.Bytes.DATETIME_SIZE;
import static com.vaticle.typedb.core.common.collection.Bytes.DOUBLE_SIZE;
import static com.vaticle.typedb.core.common.collection.Bytes.LONG_SIZE;
import static com.vaticle.typedb.core.common.collection.Bytes.booleanToByte;
import static com.vaticle.typedb.core.common.collection.Bytes.byteToBoolean;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_IID_CASTING;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.BOOLEAN;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.DATETIME;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.DOUBLE;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.LONG;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING_ENCODING;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING_MAX_SIZE;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING_SIZE_ENCODING;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.TIME_ZONE_ID;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Thing.ATTRIBUTE;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Type.ATTRIBUTE_TYPE;

public abstract class VertexIID extends PartitionedIID {

    private static final Key.Partition PARTITION = Key.Partition.DEFAULT;

    private VertexIID(ByteArray bytes) {
        super(bytes);
    }

    public static VertexIID of(ByteArray bytes) {
        switch (Encoding.Prefix.of(bytes.get(0)).type()) {
            case TYPE:
                return VertexIID.Type.of(bytes);
            case THING:
                return VertexIID.Thing.of(bytes);
            default:
                return null;
        }
    }

    public abstract Encoding.Vertex encoding();

    public PrefixIID prefix() {
        return PrefixIID.of(encoding());
    }

    @Override
    public Partition partition() {
        return PARTITION;
    }

    public static class Type extends VertexIID {

        public static final int LENGTH = PrefixIID.LENGTH + 2;

        private Type(ByteArray bytes) {
            super(bytes);
            assert bytes.length() == LENGTH;
        }

        public static VertexIID.Type of(ByteArray bytes) {
            return new Type(bytes);
        }

        public static VertexIID.Type extract(ByteArray bytes, int from) {
            return new Type(bytes.view(from, from + LENGTH));
        }

        public static Key.Prefix<VertexIID.Type> prefix(Encoding.Vertex.Type encoding) {
            return new Key.Prefix<>(encoding.prefix().bytes(), PARTITION, (bytes) -> extract(bytes, 0));
        }

        public boolean isType() {
            return true;
        }

        public VertexIID.Type asType() {
            return this;
        }

        /**
         * Generate an IID for a {@code TypeVertex} for a given {@code Encoding}
         *
         * @param keyGenerator to generate the IID for a {@code TypeVertex}
         * @param encoding     of the {@code TypeVertex} in which the IID will be used for
         * @return a byte array representing a new IID for a {@code TypeVertex}
         */
        public static VertexIID.Type generate(KeyGenerator.Schema keyGenerator, Encoding.Vertex.Type encoding) {
            return of(join(encoding.prefix().bytes(), keyGenerator.forType(PrefixIID.of(encoding), encoding.root().properLabel())));
        }

        @Override
        public Encoding.Vertex.Type encoding() {
            return Encoding.Vertex.Type.of(bytes.get(0));
        }

        @Override
        public String toString() {
            if (readableString == null) {
                readableString = "[" + PrefixIID.LENGTH + ": " + encoding().toString() + "][" +
                        (VertexIID.Type.LENGTH - PrefixIID.LENGTH) + ": " +
                        bytes.view(PrefixIID.LENGTH, VertexIID.Type.LENGTH).decodeSortedAsShort() + "]" +
                        "[partition: " + partition() + "]";
            }
            return readableString;
        }
    }


    public static class Thing extends VertexIID {

        public static final int PREFIX_W_TYPE_LENGTH = PrefixIID.LENGTH + VertexIID.Type.LENGTH;
        public static final int DEFAULT_LENGTH = PREFIX_W_TYPE_LENGTH + LONG_SIZE;

        private Thing(ByteArray bytes) {
            super(bytes);
        }

        /**
         * Generate an IID for a {@code ThingVertex} for a given {@code Encoding} and {@code TypeVertex}
         *
         * @param keyGenerator to generate the IID for a {@code ThingVertex}
         * @param typeIID      {@code IID} of the {@code TypeVertex} in which this {@code ThingVertex} is an instance of
         * @param typeLabel    {@code Label} of the {@code TypeVertex} in which this {@code ThingVertex} is an instance of
         * @return a byte array representing a new IID for a {@code ThingVertex}
         */
        public static VertexIID.Thing generate(KeyGenerator.Data keyGenerator, Type typeIID, Label typeLabel) {
            return new Thing(join(typeIID.encoding().instance().prefix().bytes(),
                    typeIID.bytes, keyGenerator.forThing(typeIID, typeLabel)));
        }

        public static VertexIID.Thing of(ByteArray bytes) {
            if (Encoding.Vertex.Type.of(bytes.get(PrefixIID.LENGTH)).equals(ATTRIBUTE_TYPE)) {
                return VertexIID.Attribute.of(bytes);
            } else {
                return new VertexIID.Thing(bytes);
            }
        }

        public static VertexIID.Thing extract(ByteArray bytes, int from) {
            if (Encoding.Vertex.Thing.of(bytes.get(from)).equals(ATTRIBUTE)) {
                return VertexIID.Attribute.extract(bytes, from);
            } else {
                return new VertexIID.Thing(bytes.view(from, from + DEFAULT_LENGTH));
            }
        }

        public static Key.Prefix<Thing> prefix(Type typeIID) {
            return new Key.Prefix<>(join(typeIID.encoding().instance().prefix().bytes(), typeIID.bytes),
                    PARTITION, Thing::of);
        }

        public Type type() {
            return Type.of(bytes.view(PrefixIID.LENGTH, PREFIX_W_TYPE_LENGTH));
        }

        public Encoding.Vertex.Thing encoding() {
            return Encoding.Vertex.Thing.of(bytes.get(0));
        }

        public ByteArray key() {
            return bytes.view(PREFIX_W_TYPE_LENGTH);
        }

        public boolean isAttribute() {
            return false;
        }

        public VertexIID.Attribute<?> asAttribute() {
            throw TypeDBException.of(INVALID_THING_IID_CASTING, className(VertexIID.Attribute.class));
        }

        @Override
        public String toString() {
            if (readableString == null) {
                readableString = "[" + PrefixIID.LENGTH + ": " + encoding().toString() + "]" +
                        "[" + VertexIID.Type.LENGTH + ": " + type().toString() + "]" +
                        "[" + (DEFAULT_LENGTH - PREFIX_W_TYPE_LENGTH) + ": " +
                        bytes.view(PREFIX_W_TYPE_LENGTH, DEFAULT_LENGTH).decodeSortedAsLong() + "]" +
                        "[partition: " + partition() + "]";
            }
            return readableString;
        }
    }

    public static abstract class Attribute<VALUE> extends VertexIID.Thing {

        public static final int VALUE_TYPE_LENGTH = 1;
        static final int VALUE_TYPE_INDEX = PrefixIID.LENGTH + VertexIID.Type.LENGTH;
        static final int VALUE_INDEX = VALUE_TYPE_INDEX + VALUE_TYPE_LENGTH;
        private final Encoding.ValueType<VALUE> valueType;

        private Attribute(ByteArray bytes, Encoding.ValueType<VALUE> valueType) {
            super(bytes);
            assert bytes.get(PREFIX_W_TYPE_LENGTH) == valueType.key();
            this.valueType = valueType;
        }

        Attribute(Encoding.ValueType<VALUE> valueType, Type typeIID, ByteArray valueBytes) {
            super(join(
                    ATTRIBUTE.prefix().bytes(),
                    typeIID.bytes,
                    valueType.bytes(),
                    valueBytes
            ));
            this.valueType = valueType;
        }

        public static VertexIID.Attribute<?> of(ByteArray bytes) {
            Encoding.ValueType<?> valueType = Encoding.ValueType.of(bytes.get(PREFIX_W_TYPE_LENGTH));
            if (valueType == BOOLEAN) return new Boolean(bytes);
            else if (valueType == LONG) return new Long(bytes);
            else if (valueType == DOUBLE) return new Double(bytes);
            else if (valueType == STRING) return new String(bytes);
            else if (valueType == DATETIME) return new DateTime(bytes);
            assert false;
            throw TypeDBException.of(UNRECOGNISED_VALUE);
        }

        public static VertexIID.Attribute<?> extract(ByteArray bytes, int from) {
            Encoding.ValueType<?> valueType = Encoding.ValueType.of(bytes.get(from + VALUE_TYPE_INDEX));
            if (valueType == BOOLEAN) return Boolean.extract(bytes, from);
            else if (valueType == LONG) return Long.extract(bytes, from);
            else if (valueType == DOUBLE) return Double.extract(bytes, from);
            else if (valueType == STRING) return String.extract(bytes, from);
            else if (valueType == DATETIME) return DateTime.extract(bytes, from);
            assert false;
            throw TypeDBException.of(UNRECOGNISED_VALUE);
        }

        public abstract VALUE value();

        public Encoding.ValueType<VALUE> valueType() {
            return valueType;
        }

        public VertexIID.Attribute.Boolean asBoolean() {
            throw TypeDBException.of(INVALID_THING_IID_CASTING, className(Boolean.class));
        }

        public VertexIID.Attribute.Long asLong() {
            throw TypeDBException.of(INVALID_THING_IID_CASTING, className(Long.class));
        }

        public VertexIID.Attribute.Double asDouble() {
            throw TypeDBException.of(INVALID_THING_IID_CASTING, className(Double.class));
        }

        public VertexIID.Attribute.String asString() {
            throw TypeDBException.of(INVALID_THING_IID_CASTING, className(String.class));
        }

        public VertexIID.Attribute.DateTime asDateTime() {
            throw TypeDBException.of(INVALID_THING_IID_CASTING, className(DateTime.class));
        }

        @Override
        public boolean isAttribute() {
            return true;
        }

        public VertexIID.Attribute<?> asAttribute() {
            return this;
        }

        @Override
        public java.lang.String toString() {
            if (readableString == null) {
                readableString = "[" + PrefixIID.LENGTH + ": " + ATTRIBUTE.toString() + "]" +
                        "[" + VertexIID.Type.LENGTH + ": " + type().toString() + "]" +
                        "[" + VALUE_TYPE_LENGTH + ": " + valueType().toString() + "]" +
                        "[" + (bytes.length() - VALUE_INDEX) + ": " + value().toString() + "]" +
                        "[partition: " + partition() + "]";
            }
            return readableString;
        }

        public static class Boolean extends Attribute<java.lang.Boolean> {

            private Boolean(ByteArray bytes) {
                super(bytes, BOOLEAN);
            }

            public Boolean(VertexIID.Type typeIID, boolean value) {
                super(BOOLEAN, typeIID, ByteArray.of(new byte[]{booleanToByte(value)}));
            }

            public static VertexIID.Attribute.Boolean extract(ByteArray bytes, int from) {
                return new VertexIID.Attribute.Boolean(bytes.view(from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + 1));
            }

            @Override
            public java.lang.Boolean value() {
                return byteToBoolean(bytes.get(VALUE_INDEX));
            }

            @Override
            public Boolean asBoolean() {
                return this;
            }
        }

        public static class Long extends Attribute<java.lang.Long> {

            private Long(ByteArray bytes) {
                super(bytes, LONG);
            }

            public Long(VertexIID.Type typeIID, long value) {
                super(LONG, typeIID, encodeLongAsSorted(value));
            }

            public static VertexIID.Attribute.Long extract(ByteArray bytes, int from) {
                return new VertexIID.Attribute.Long(bytes.view(from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + LONG_SIZE));
            }

            @Override
            public java.lang.Long value() {
                return bytes.view(VALUE_INDEX, VALUE_INDEX + LONG_SIZE).decodeSortedAsLong();
            }

            @Override
            public Long asLong() {
                return this;
            }
        }

        public static class Double extends Attribute<java.lang.Double> {

            private Double(ByteArray bytes) {
                super(bytes, DOUBLE);
            }

            public Double(VertexIID.Type typeIID, double value) {
                super(DOUBLE, typeIID, encodeDoubleAsSorted(value));
            }

            public static VertexIID.Attribute.Double extract(ByteArray bytes, int from) {
                return new VertexIID.Attribute.Double(bytes.view(from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + DOUBLE_SIZE));
            }

            @Override
            public java.lang.Double value() {
                return bytes.view(VALUE_INDEX, VALUE_INDEX + DOUBLE_SIZE).decodeSortedAsDouble();
            }

            @Override
            public Double asDouble() {
                return this;
            }
        }

        public static class String extends Attribute<java.lang.String> {

            private String(ByteArray bytes) {
                super(bytes, STRING);
            }

            public String(VertexIID.Type typeIID, java.lang.String value) throws TypeDBCheckedException {
                super(STRING, typeIID, encodeStringAsSorted(value, STRING_ENCODING));
                assert bytes.length() <= STRING_MAX_SIZE + STRING_SIZE_ENCODING;
            }

            public static VertexIID.Attribute.String extract(ByteArray bytes, int from) {
                int attValIndex = from + VALUE_INDEX;
                int strValLen = bytes.view(attValIndex, attValIndex + STRING_SIZE_ENCODING).decodeUnsignedShort();
                int stringEnd = from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + STRING_SIZE_ENCODING + strValLen;
                return new VertexIID.Attribute.String(bytes.view(from, stringEnd));
            }

            @Override
            public java.lang.String value() {
                return bytes.view(VALUE_INDEX, bytes.length()).decodeSortedAsString(STRING_ENCODING);
            }

            @Override
            public String asString() {
                return this;
            }
        }

        public static class DateTime extends Attribute<java.time.LocalDateTime> {

            private DateTime(ByteArray bytes) {
                super(bytes, DATETIME);
            }

            public DateTime(VertexIID.Type typeIID, java.time.LocalDateTime value) {
                super(DATETIME, typeIID, encodeDateTimeAsSorted(value, TIME_ZONE_ID));
            }

            public static VertexIID.Attribute.DateTime extract(ByteArray bytes, int from) {
                return new VertexIID.Attribute.DateTime(bytes.view(from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + DATETIME_SIZE));
            }

            @Override
            public java.time.LocalDateTime value() {
                return bytes.view(VALUE_INDEX, bytes.length()).decodeSortedAsDateTime(TIME_ZONE_ID);
            }

            @Override
            public DateTime asDateTime() {
                return this;
            }
        }
    }
}
