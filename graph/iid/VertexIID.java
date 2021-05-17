/*
 * Copyright (C) 2021 Vaticle
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

import com.vaticle.typedb.core.common.exception.TypeDBCheckedException;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.util.ByteArray;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.Encoding.ValueEncoding;
import com.vaticle.typedb.core.graph.common.KeyGenerator;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.collection.Bytes.DATETIME_SIZE;
import static com.vaticle.typedb.core.common.collection.Bytes.DOUBLE_SIZE;
import static com.vaticle.typedb.core.common.collection.Bytes.LONG_SIZE;
import static com.vaticle.typedb.core.common.collection.Bytes.booleanToByte;
import static com.vaticle.typedb.core.common.collection.Bytes.byteToBoolean;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_IID_CASTING;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING_ENCODING;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING_MAX_SIZE;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING_SIZE_ENCODING;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.TIME_ZONE_ID;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Thing.ATTRIBUTE;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Type.ATTRIBUTE_TYPE;

public abstract class VertexIID extends IID {

    VertexIID(ByteArray bytes) {
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

    public static class Type extends VertexIID {

        public static final int LENGTH = PrefixIID.LENGTH + 2;

        Type(ByteArray bytes) {
            super(bytes);
        }

        public static VertexIID.Type of(ByteArray bytes) {
            return new Type(bytes);
        }

        public static VertexIID.Type extract(ByteArray bytes, int from) {
            return new Type(bytes.copyRange(from, from + LENGTH));
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
            return of(ByteArray.join(encoding.prefix().bytes(), keyGenerator.forType(PrefixIID.of(encoding), encoding.root().properLabel())));
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
                        ValueEncoding.bytesToShort(bytes.view(PrefixIID.LENGTH, VertexIID.Type.LENGTH)) + "]";
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
            return new Thing(ByteArray.join(typeIID.encoding().instance().prefix().bytes(),
                                            typeIID.bytes(), keyGenerator.forThing(typeIID, typeLabel)));
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
                return new VertexIID.Thing(bytes.copyRange(from, from + DEFAULT_LENGTH));
            }
        }

        public Type type() {
            return Type.of(bytes.copyRange(PrefixIID.LENGTH, PREFIX_W_TYPE_LENGTH));
        }

        public Encoding.Vertex.Thing encoding() {
            return Encoding.Vertex.Thing.of(bytes.get(0));
        }

        public ByteArray key() {
            return bytes.copyRange(PREFIX_W_TYPE_LENGTH);
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
                        ValueEncoding.bytesToLong(bytes.view(PREFIX_W_TYPE_LENGTH, DEFAULT_LENGTH)) + "]";
            }
            return readableString;
        }
    }

    public static abstract class Attribute<VALUE> extends VertexIID.Thing {

        static final int VALUE_TYPE_LENGTH = 1;
        static final int VALUE_TYPE_INDEX = PrefixIID.LENGTH + VertexIID.Type.LENGTH;
        static final int VALUE_INDEX = VALUE_TYPE_INDEX + VALUE_TYPE_LENGTH;
        private final Encoding.ValueType valueType;

        Attribute(ByteArray bytes) {
            super(bytes);
            valueType = Encoding.ValueType.of(bytes.get(PREFIX_W_TYPE_LENGTH));
        }

        Attribute(Encoding.ValueType valueType, Type typeIID, ByteArray valueBytes) {
            super(ByteArray.join(
                    ATTRIBUTE.prefix().bytes(),
                    typeIID.bytes(),
                    valueType.bytes(),
                    valueBytes
            ));
            this.valueType = valueType;
        }

        public static VertexIID.Attribute<?> of(ByteArray bytes) {
            switch (Encoding.ValueType.of(bytes.get(PREFIX_W_TYPE_LENGTH))) {
                case BOOLEAN:
                    return new Attribute.Boolean(bytes);
                case LONG:
                    return new Attribute.Long(bytes);
                case DOUBLE:
                    return new Attribute.Double(bytes);
                case STRING:
                    return new Attribute.String(bytes);
                case DATETIME:
                    return new Attribute.DateTime(bytes);
                default:
                    assert false;
                    throw TypeDBException.of(UNRECOGNISED_VALUE);
            }
        }

        public static VertexIID.Attribute<?> extract(ByteArray bytes, int from) {
            switch (Encoding.ValueType.of(bytes.get(from + VALUE_TYPE_INDEX))) {
                case BOOLEAN:
                    return VertexIID.Attribute.Boolean.extract(bytes, from);
                case LONG:
                    return VertexIID.Attribute.Long.extract(bytes, from);
                case DOUBLE:
                    return VertexIID.Attribute.Double.extract(bytes, from);
                case STRING:
                    return VertexIID.Attribute.String.extract(bytes, from);
                case DATETIME:
                    return VertexIID.Attribute.DateTime.extract(bytes, from);
                default:
                    assert false;
                    throw TypeDBException.of(UNRECOGNISED_VALUE);
            }
        }

        public abstract VALUE value();

        public Encoding.ValueType valueType() {
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
                        "[" + (bytes.length() - VALUE_INDEX) + ": " + value().toString() + "]";
            }
            return readableString;
        }

        public static class Boolean extends Attribute<java.lang.Boolean> {

            public Boolean(ByteArray bytes) {
                super(bytes);
            }

            public Boolean(VertexIID.Type typeIID, boolean value) {
                super(Encoding.ValueType.BOOLEAN, typeIID, ByteArray.of(new byte[]{booleanToByte(value)}));
            }

            public static VertexIID.Attribute.Boolean extract(ByteArray bytes, int from) {
                return new VertexIID.Attribute.Boolean(bytes.copyRange(from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + 1));
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

            public Long(ByteArray bytes) {
                super(bytes);
            }

            public Long(VertexIID.Type typeIID, long value) {
                super(Encoding.ValueType.LONG, typeIID, ValueEncoding.longToBytes(value));
            }

            public static VertexIID.Attribute.Long extract(ByteArray bytes, int from) {
                return new VertexIID.Attribute.Long(bytes.copyRange(from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + LONG_SIZE));
            }

            @Override
            public java.lang.Long value() {
                return ValueEncoding.bytesToLong(bytes.view(VALUE_INDEX, VALUE_INDEX + LONG_SIZE));
            }

            @Override
            public Long asLong() {
                return this;
            }
        }

        public static class Double extends Attribute<java.lang.Double> {

            public Double(ByteArray bytes) {
                super(bytes);
            }

            public Double(VertexIID.Type typeIID, double value) {
                super(Encoding.ValueType.DOUBLE, typeIID, ValueEncoding.doubleToBytes(value));
            }

            public static VertexIID.Attribute.Double extract(ByteArray bytes, int from) {
                return new VertexIID.Attribute.Double(bytes.copyRange(from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + DOUBLE_SIZE));
            }

            @Override
            public java.lang.Double value() {
                return ValueEncoding.bytesToDouble(bytes.view(VALUE_INDEX, VALUE_INDEX + DOUBLE_SIZE));
            }

            @Override
            public Double asDouble() {
                return this;
            }
        }

        public static class String extends Attribute<java.lang.String> {

            public String(ByteArray bytes) {
                super(bytes);
            }

            public String(VertexIID.Type typeIID, java.lang.String value) throws TypeDBCheckedException {
                super(Encoding.ValueType.STRING, typeIID, ValueEncoding.stringToBytes(value, STRING_ENCODING));
                assert bytes.length() <= STRING_MAX_SIZE + STRING_SIZE_ENCODING;
            }

            public static VertexIID.Attribute.String extract(ByteArray bytes, int from) {
                int attValIndex = from + VALUE_INDEX;
                int strValLen = bytes.view(attValIndex, attValIndex + STRING_SIZE_ENCODING).decodeUnsignedShort();
                int stringEnd = from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + STRING_SIZE_ENCODING + strValLen;
                return new VertexIID.Attribute.String(bytes.copyRange(from, stringEnd));
            }

            @Override
            public java.lang.String value() {
                return ValueEncoding.bytesToString(bytes.view(VALUE_INDEX, bytes.length()), STRING_ENCODING);
            }

            @Override
            public String asString() {
                return this;
            }
        }

        public static class DateTime extends Attribute<java.time.LocalDateTime> {

            public DateTime(ByteArray bytes) {
                super(bytes);
            }

            public DateTime(VertexIID.Type typeIID, java.time.LocalDateTime value) {
                super(Encoding.ValueType.DATETIME, typeIID, ValueEncoding.dateTimeToBytes(value, TIME_ZONE_ID));
            }

            public static VertexIID.Attribute.DateTime extract(ByteArray bytes, int from) {
                return new VertexIID.Attribute.DateTime(bytes.copyRange(from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + DATETIME_SIZE));
            }

            @Override
            public java.time.LocalDateTime value() {
                return ValueEncoding.bytesToDateTime(bytes.view(VALUE_INDEX, bytes.length()), TIME_ZONE_ID);
            }

            @Override
            public DateTime asDateTime() {
                return this;
            }
        }
    }
}
