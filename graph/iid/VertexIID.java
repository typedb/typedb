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

package grakn.core.graph.iid;

import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.util.KeyGenerator;

import static grakn.common.util.Objects.className;
import static grakn.core.common.collection.Bytes.DATETIME_SIZE;
import static grakn.core.common.collection.Bytes.DOUBLE_SIZE;
import static grakn.core.common.collection.Bytes.LONG_SIZE;
import static grakn.core.common.collection.Bytes.booleanToByte;
import static grakn.core.common.collection.Bytes.byteToBoolean;
import static grakn.core.common.collection.Bytes.bytesToDateTime;
import static grakn.core.common.collection.Bytes.bytesToString;
import static grakn.core.common.collection.Bytes.dateTimeToBytes;
import static grakn.core.common.collection.Bytes.doubleToSortedBytes;
import static grakn.core.common.collection.Bytes.join;
import static grakn.core.common.collection.Bytes.longToSortedBytes;
import static grakn.core.common.collection.Bytes.sortedBytesToDouble;
import static grakn.core.common.collection.Bytes.sortedBytesToLong;
import static grakn.core.common.collection.Bytes.sortedBytesToShort;
import static grakn.core.common.collection.Bytes.stringToBytes;
import static grakn.core.common.collection.Bytes.unsignedByteToInt;
import static grakn.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static grakn.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_IID_CASTING;
import static grakn.core.graph.util.Encoding.STRING_ENCODING;
import static grakn.core.graph.util.Encoding.STRING_MAX_LENGTH;
import static grakn.core.graph.util.Encoding.TIME_ZONE_ID;
import static grakn.core.graph.util.Encoding.Vertex.Thing.ATTRIBUTE;
import static grakn.core.graph.util.Encoding.Vertex.Type.ATTRIBUTE_TYPE;
import static java.util.Arrays.copyOfRange;

public abstract class VertexIID extends IID {

    VertexIID(byte[] bytes) {
        super(bytes);
    }

    public static VertexIID of(byte[] bytes) {
        switch (Encoding.Prefix.of(bytes[0]).type()) {
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

        Type(byte[] bytes) {
            super(bytes);
        }

        public static VertexIID.Type of(byte[] bytes) {
            return new Type(bytes);
        }

        static VertexIID.Type extract(byte[] bytes, int from) {
            return new Type(copyOfRange(bytes, from, from + LENGTH));
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
            return Encoding.Vertex.Type.of(bytes[0]);
        }

        @Override
        public String toString() {
            if (readableString == null) {
                readableString = "[" + PrefixIID.LENGTH + ": " + encoding().toString() + "][" +
                        (VertexIID.Type.LENGTH - PrefixIID.LENGTH) + ": " +
                        sortedBytesToShort(copyOfRange(bytes, PrefixIID.LENGTH, VertexIID.Type.LENGTH)) + "]";
            }
            return readableString;
        }
    }


    public static class Thing extends VertexIID {

        public static final int PREFIX_W_TYPE_LENGTH = PrefixIID.LENGTH + VertexIID.Type.LENGTH;
        public static final int DEFAULT_LENGTH = PREFIX_W_TYPE_LENGTH + LONG_SIZE;

        private Thing(byte[] bytes) {
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
                                  typeIID.bytes(), keyGenerator.forThing(typeIID, typeLabel)));
        }

        public static VertexIID.Thing of(byte[] bytes) {
            if (Encoding.Vertex.Type.of(bytes[PrefixIID.LENGTH]).equals(ATTRIBUTE_TYPE)) {
                return VertexIID.Attribute.of(bytes);
            } else {
                return new VertexIID.Thing(bytes);
            }
        }

        public static VertexIID.Thing extract(byte[] bytes, int from) {
            if (Encoding.Vertex.Thing.of(bytes[from]).equals(ATTRIBUTE)) {
                return VertexIID.Attribute.extract(bytes, from);
            } else {
                return new VertexIID.Thing(copyOfRange(bytes, from, from + DEFAULT_LENGTH));
            }
        }

        public Type type() {
            return Type.of(copyOfRange(bytes, PrefixIID.LENGTH, PREFIX_W_TYPE_LENGTH));
        }

        public Encoding.Vertex.Thing encoding() {
            return Encoding.Vertex.Thing.of(bytes[0]);
        }

        public byte[] key() {
            return copyOfRange(bytes, PREFIX_W_TYPE_LENGTH, bytes.length);
        }

        public boolean isAttribute() {
            return false;
        }

        public VertexIID.Attribute<?> asAttribute() {
            throw GraknException.of(INVALID_THING_IID_CASTING, className(VertexIID.Attribute.class));
        }

        @Override
        public String toString() {
            if (readableString == null) {
                readableString = "[" + PrefixIID.LENGTH + ": " + encoding().toString() + "]" +
                        "[" + VertexIID.Type.LENGTH + ": " + type().toString() + "]" +
                        "[" + (DEFAULT_LENGTH - PREFIX_W_TYPE_LENGTH) + ": " +
                        sortedBytesToLong(copyOfRange(bytes, PREFIX_W_TYPE_LENGTH, DEFAULT_LENGTH)) + "]";
            }
            return readableString;
        }
    }

    public static abstract class Attribute<VALUE> extends VertexIID.Thing {

        static final int VALUE_TYPE_LENGTH = 1;
        static final int VALUE_TYPE_INDEX = PrefixIID.LENGTH + VertexIID.Type.LENGTH;
        static final int VALUE_INDEX = VALUE_TYPE_INDEX + VALUE_TYPE_LENGTH;
        private final Encoding.ValueType valueType;

        Attribute(byte[] bytes) {
            super(bytes);
            valueType = Encoding.ValueType.of(bytes[PREFIX_W_TYPE_LENGTH]);
        }

        Attribute(Encoding.ValueType valueType, VertexIID.Type typeIID, byte[] valueBytes) {
            super(join(
                    ATTRIBUTE.prefix().bytes(),
                    typeIID.bytes(),
                    valueType.bytes(),
                    valueBytes
            ));
            this.valueType = valueType;
        }

        public static VertexIID.Attribute<?> of(byte[] bytes) {
            switch (Encoding.ValueType.of(bytes[PREFIX_W_TYPE_LENGTH])) {
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
                    throw GraknException.of(UNRECOGNISED_VALUE);
            }
        }

        public static VertexIID.Attribute<?> extract(byte[] bytes, int from) {
            switch (Encoding.ValueType.of(bytes[from + VALUE_TYPE_INDEX])) {
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
                    throw GraknException.of(UNRECOGNISED_VALUE);
            }
        }

        public abstract VALUE value();

        public Encoding.ValueType valueType() {
            return valueType;
        }

        public VertexIID.Attribute.Boolean asBoolean() {
            throw GraknException.of(INVALID_THING_IID_CASTING, className(Boolean.class));
        }

        public VertexIID.Attribute.Long asLong() {
            throw GraknException.of(INVALID_THING_IID_CASTING, className(Long.class));
        }

        public VertexIID.Attribute.Double asDouble() {
            throw GraknException.of(INVALID_THING_IID_CASTING, className(Double.class));
        }

        public VertexIID.Attribute.String asString() {
            throw GraknException.of(INVALID_THING_IID_CASTING, className(String.class));
        }

        public VertexIID.Attribute.DateTime asDateTime() {
            throw GraknException.of(INVALID_THING_IID_CASTING, className(DateTime.class));
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
                        "[" + (bytes.length - VALUE_INDEX) + ": " + value().toString() + "]";
            }
            return readableString;
        }

        public static class Boolean extends Attribute<java.lang.Boolean> {

            public Boolean(byte[] bytes) {
                super(bytes);
            }

            public Boolean(VertexIID.Type typeIID, boolean value) {
                super(Encoding.ValueType.BOOLEAN, typeIID, new byte[]{booleanToByte(value)});
            }

            public static VertexIID.Attribute.Boolean extract(byte[] bytes, int from) {
                return new VertexIID.Attribute.Boolean(copyOfRange(bytes, from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + 1));
            }

            @Override
            public java.lang.Boolean value() {
                return byteToBoolean(bytes[VALUE_INDEX]);
            }

            @Override
            public Boolean asBoolean() {
                return this;
            }
        }

        public static class Long extends Attribute<java.lang.Long> {

            public Long(byte[] bytes) {
                super(bytes);
            }

            public Long(VertexIID.Type typeIID, long value) {
                super(Encoding.ValueType.LONG, typeIID, longToSortedBytes(value));
            }

            public static VertexIID.Attribute.Long extract(byte[] bytes, int from) {
                return new VertexIID.Attribute.Long(copyOfRange(bytes, from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + LONG_SIZE));
            }

            @Override
            public java.lang.Long value() {
                return sortedBytesToLong(copyOfRange(bytes, VALUE_INDEX, VALUE_INDEX + LONG_SIZE));
            }

            @Override
            public Long asLong() {
                return this;
            }
        }

        public static class Double extends Attribute<java.lang.Double> {

            public Double(byte[] bytes) {
                super(bytes);
            }

            public Double(VertexIID.Type typeIID, double value) {
                super(Encoding.ValueType.DOUBLE, typeIID, doubleToSortedBytes(value));
            }

            public static VertexIID.Attribute.Double extract(byte[] bytes, int from) {
                return new VertexIID.Attribute.Double(copyOfRange(bytes, from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + DOUBLE_SIZE));
            }

            @Override
            public java.lang.Double value() {
                return sortedBytesToDouble(copyOfRange(bytes, VALUE_INDEX, VALUE_INDEX + DOUBLE_SIZE));
            }

            @Override
            public Double asDouble() {
                return this;
            }
        }

        public static class String extends Attribute<java.lang.String> {

            public String(byte[] bytes) {
                super(bytes);
            }

            public String(VertexIID.Type typeIID, java.lang.String value) {
                super(Encoding.ValueType.STRING, typeIID, stringToBytes(value, STRING_ENCODING));
                assert bytes.length <= STRING_MAX_LENGTH + 1;
            }

            public static VertexIID.Attribute.String extract(byte[] bytes, int from) {
                final int valueLength = unsignedByteToInt(bytes[from + VALUE_INDEX]) + 1;
                return new VertexIID.Attribute.String(copyOfRange(bytes, from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + valueLength));
            }

            @Override
            public java.lang.String value() {
                return bytesToString(copyOfRange(bytes, VALUE_INDEX, bytes.length), STRING_ENCODING);
            }

            @Override
            public String asString() {
                return this;
            }
        }

        public static class DateTime extends Attribute<java.time.LocalDateTime> {

            public DateTime(byte[] bytes) {
                super(bytes);
            }

            public DateTime(VertexIID.Type typeIID, java.time.LocalDateTime value) {
                super(Encoding.ValueType.DATETIME, typeIID, dateTimeToBytes(value, TIME_ZONE_ID));
            }

            public static VertexIID.Attribute.DateTime extract(byte[] bytes, int from) {
                return new VertexIID.Attribute.DateTime(copyOfRange(bytes, from, from + PREFIX_W_TYPE_LENGTH + VALUE_TYPE_LENGTH + DATETIME_SIZE));
            }

            @Override
            public java.time.LocalDateTime value() {
                return bytesToDateTime(copyOfRange(bytes, VALUE_INDEX, bytes.length), TIME_ZONE_ID);
            }

            @Override
            public DateTime asDateTime() {
                return this;
            }
        }
    }
}
