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

package com.vaticle.typedb.core.encoding.iid;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBCheckedException;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.key.Key;
import com.vaticle.typedb.core.encoding.key.KeyGenerator;

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
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_IID_CASTING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ValueRead.INVALID_VALUE_IID_CASTING;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.BOOLEAN;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DATETIME;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DOUBLE;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.LONG;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.STRING;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.STRING_ENCODING;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.STRING_MAX_SIZE;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.STRING_SIZE_ENCODING;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.TIME_ZONE_ID;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Thing.ATTRIBUTE;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.ATTRIBUTE_TYPE;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Value.VALUE;

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
            case VALUE:
                return VertexIID.Value.of(bytes);
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

        public KeyIID key() {
            return KeyIID.of(bytes.view(PREFIX_W_TYPE_LENGTH));
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
        VALUE value;

        Attribute(Encoding.ValueType<VALUE> valueType, Type typeIID, ByteArray valueBytes, VALUE value) {
            this(join(ATTRIBUTE.prefix().bytes(), typeIID.bytes, valueType.bytes(), valueBytes), valueType, value);
        }

        private Attribute(ByteArray bytes, Encoding.ValueType<VALUE> valueType) {
            super(bytes);
            assert bytes.get(PREFIX_W_TYPE_LENGTH) == valueType.key();
            this.valueType = valueType;
        }

        private Attribute(ByteArray bytes, Encoding.ValueType<VALUE> valueType, VALUE value) {
            super(bytes);
            assert bytes.get(PREFIX_W_TYPE_LENGTH) == valueType.key();
            this.valueType = valueType;
            this.value = value;
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
                super(BOOLEAN, typeIID, ByteArray.of(new byte[]{booleanToByte(value)}), value);
            }

            public static VertexIID.Attribute.Boolean extract(ByteArray bytes, int from) {
                return new VertexIID.Attribute.Boolean(bytes.view(from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + 1));
            }

            @Override
            public java.lang.Boolean value() {
                if (value == null) value = byteToBoolean(bytes.get(VALUE_INDEX));
                return value;
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
                super(LONG, typeIID, encodeLongAsSorted(value), value);
            }

            public static VertexIID.Attribute.Long extract(ByteArray bytes, int from) {
                return new VertexIID.Attribute.Long(bytes.view(from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + LONG_SIZE));
            }

            @Override
            public java.lang.Long value() {
                if (value == null) value = bytes.view(VALUE_INDEX, VALUE_INDEX + LONG_SIZE).decodeSortedAsLong();
                return value;
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

            public Double(VertexIID.Type typeIID, java.lang.Double value) throws TypeDBCheckedException {
                super(DOUBLE, typeIID, encodeDoubleAsSorted(value), value);
            }

            public static VertexIID.Attribute.Double extract(ByteArray bytes, int from) {
                return new VertexIID.Attribute.Double(bytes.view(from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + DOUBLE_SIZE));
            }

            @Override
            public java.lang.Double value() {
                if (value == null) value = bytes.view(VALUE_INDEX, VALUE_INDEX + DOUBLE_SIZE).decodeSortedAsDouble();
                return value;
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
                super(STRING, typeIID, encodeStringAsSorted(value, STRING_ENCODING), value);
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
                if (value == null)
                    value = bytes.view(VALUE_INDEX, bytes.length()).decodeSortedAsString(STRING_ENCODING);
                return value;
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
                super(DATETIME, typeIID, encodeDateTimeAsSorted(value, TIME_ZONE_ID), value);
            }

            public static VertexIID.Attribute.DateTime extract(ByteArray bytes, int from) {
                return new VertexIID.Attribute.DateTime(bytes.view(from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + DATETIME_SIZE));
            }

            @Override
            public java.time.LocalDateTime value() {
                if (value == null) value = bytes.view(VALUE_INDEX, bytes.length()).decodeSortedAsDateTime(TIME_ZONE_ID);
                return value;
            }

            @Override
            public DateTime asDateTime() {
                return this;
            }
        }
    }

    public abstract static class Value<VALUE> extends VertexIID {
        private static final int VALUE_TYPE_INDEX = PrefixIID.LENGTH;
        private static final int VALUE_TYPE_LENGTH = 1;
        private static final int VALUE_INDEX = PrefixIID.LENGTH + VALUE_TYPE_LENGTH;

        private final Encoding.ValueType<VALUE> valueType;
        VALUE value;

        private Value(Encoding.ValueType<VALUE> valueType, ByteArray bytes) {
            super(bytes);
            assert bytes.get(VALUE_TYPE_INDEX) == valueType.key();
            this.valueType = valueType;
        }

        private Value(Encoding.ValueType<VALUE> valueType, ByteArray bytes, VALUE value) {
            super(bytes);
            assert bytes.get(VALUE_TYPE_INDEX) == valueType.key();
            this.valueType = valueType;
            this.value = value;
        }

        private static ByteArray valueBytesToIID(Encoding.ValueType<?> valueType, ByteArray valueBytes) {
            return join(VALUE.prefix().bytes(), valueType.bytes(), valueBytes);
        }

        public static Value<?> of(ByteArray bytes) {
            assert Encoding.Vertex.Value.of(bytes.get(0)).equals(VALUE);
            Encoding.ValueType<?> valueType = Encoding.ValueType.of(bytes.get(VALUE_TYPE_INDEX));
            if (valueType == BOOLEAN) return new Boolean(bytes);
            else if (valueType == LONG) return new Long(bytes);
            else if (valueType == DOUBLE) return new Double(bytes);
            else if (valueType == STRING) return new String(bytes);
            else if (valueType == DATETIME) return new DateTime(bytes);
            else throw TypeDBException.of(ILLEGAL_STATE);
        }

        public Encoding.ValueType<VALUE> valueType() {
            return valueType;
        }

        public abstract VALUE value();

        @Override
        public Encoding.Vertex.Value encoding() {
            return Encoding.Vertex.Value.VALUE;
        }

        @Override
        public java.lang.String toString() {
            return "ValueVertex[" + valueType + ": " + value() + "]";
        }

        public VertexIID.Value.Boolean asBoolean() {
            throw TypeDBException.of(INVALID_VALUE_IID_CASTING, className(Value.Boolean.class));
        }

        public VertexIID.Value.Long asLong() {
            throw TypeDBException.of(INVALID_VALUE_IID_CASTING, className(Value.Long.class));
        }

        public VertexIID.Value.Double asDouble() {
            throw TypeDBException.of(INVALID_VALUE_IID_CASTING, className(Value.Double.class));
        }

        public VertexIID.Value.String asString() {
            throw TypeDBException.of(INVALID_VALUE_IID_CASTING, className(Value.String.class));
        }

        public VertexIID.Value.DateTime asDateTime() {
            throw TypeDBException.of(INVALID_VALUE_IID_CASTING, className(Value.DateTime.class));
        }

        public static class Boolean extends Value<java.lang.Boolean> {

            private Boolean(ByteArray bytes) {
                super(BOOLEAN, bytes);
            }

            public Boolean(boolean value) {
                super(BOOLEAN, valueBytesToIID(BOOLEAN, ByteArray.of(new byte[]{booleanToByte(value)})), value);
            }

            @Override
            public Boolean asBoolean() {
                return this;
            }

            @Override
            public java.lang.Boolean value() {
                if (value == null) value = byteToBoolean(bytes.get(VALUE_INDEX));
                return value;
            }
        }

        public static class Long extends Value<java.lang.Long> {

            private Long(ByteArray bytes) {
                super(LONG, bytes);
            }

            public Long(long value) {
                super(LONG, valueBytesToIID(LONG, encodeLongAsSorted(value)), value);
            }

            @Override
            public Long asLong() {
                return this;
            }

            @Override
            public java.lang.Long value() {
                if (value == null) value = bytes.view(VALUE_INDEX, VALUE_INDEX + LONG_SIZE).decodeSortedAsLong();
                return value;
            }
        }

        public static class Double extends Value<java.lang.Double> {

            private Double(ByteArray bytes) {
                super(DOUBLE, bytes);
            }

            public Double(java.lang.Double value) throws TypeDBCheckedException {
                super(DOUBLE, valueBytesToIID(DOUBLE, encodeDoubleAsSorted(value)), value);
            }
            @Override
            public Double asDouble() {
                return this;
            }

            @Override
            public java.lang.Double value() {
                if (value == null) value = bytes.view(VALUE_INDEX, VALUE_INDEX + DOUBLE_SIZE).decodeSortedAsDouble();
                return value;
            }
        }

        public static class String extends Value<java.lang.String> {

            private String(ByteArray bytes) {
                super(STRING, bytes);
            }

            public String(java.lang.String value) throws TypeDBCheckedException {
                super(STRING, valueBytesToIID(STRING, encodeStringAsSorted(value, STRING_ENCODING)), value);
            }

            @Override
            public String asString() {
                return this;
            }

            @Override
            public java.lang.String value() {
                if (value == null) value = bytes.view(VALUE_INDEX).decodeSortedAsString(STRING_ENCODING);
                return value;
            }
        }

        public static class DateTime extends Value<java.time.LocalDateTime> {

            private DateTime(ByteArray bytes) {
                super(DATETIME, bytes);
            }

            public DateTime(java.time.LocalDateTime value) {
                super(DATETIME, valueBytesToIID(DATETIME, encodeDateTimeAsSorted(value, TIME_ZONE_ID)), value);
            }

            @Override
            public DateTime asDateTime() {
                return this;
            }

            @Override
            public java.time.LocalDateTime value() {
                if (value == null) value = bytes.view(VALUE_INDEX, bytes.length()).decodeSortedAsDateTime(TIME_ZONE_ID);
                return value;
            }
        }
    }
}
