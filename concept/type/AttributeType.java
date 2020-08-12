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

package grakn.core.concept.type;

import grakn.core.concept.thing.Attribute;
import grakn.core.graph.util.Schema;

import java.time.LocalDateTime;
import java.util.stream.Stream;

public interface AttributeType extends ThingType {

    @Override
    default AttributeType asAttributeType() { return this; }

    @Override
    AttributeType getSup();

    @Override
    Stream<? extends AttributeType> getSups();

    @Override
    Stream<? extends AttributeType> getSubs();

    @Override
    Stream<? extends Attribute> getInstances();

    void setSup(AttributeType superType);

    boolean isKeyable();

    ValueType getValueType();

    AttributeType asObject();

    AttributeType.Boolean asBoolean();

    AttributeType.Long asLong();

    AttributeType.Double asDouble();

    AttributeType.String asString();

    AttributeType.DateTime asDateTime();

    enum ValueType {
        OBJECT(Schema.ValueType.OBJECT),
        BOOLEAN(Schema.ValueType.BOOLEAN),
        LONG(Schema.ValueType.LONG),
        DOUBLE(Schema.ValueType.DOUBLE),
        STRING(Schema.ValueType.STRING),
        DATETIME(Schema.ValueType.DATETIME);

        private final Class<?> valueClass;
        private final boolean isWritable;

        ValueType(Schema.ValueType valueType) {
            this.valueClass = valueType.valueClass();
            this.isWritable = valueType.isWritable();
        }

        public ValueType of(Class<?> valueClass) {
            for (ValueType vt : ValueType.values()) {
                if (vt.valueClass.equals(valueClass)) {
                    return vt;
                }
            }
            return null;
        }

        public Class<?> getValueClass() {
            return valueClass;
        }

        public boolean isWritable() {
            return isWritable;
        }
    }

    interface Boolean extends AttributeType {

        @Override
        AttributeType.Boolean getSup();

        @Override
        Stream<? extends AttributeType.Boolean> getSups();

        @Override
        Stream<? extends AttributeType.Boolean> getSubs();

        @Override
        Stream<? extends Attribute.Boolean> getInstances();

        Attribute.Boolean put(boolean value);

        Attribute.Boolean put(boolean value, boolean isInferred);

        Attribute.Boolean get(boolean value);
    }

    interface Long extends AttributeType {

        @Override
        AttributeType.Long getSup();

        @Override
        Stream<? extends AttributeType.Long> getSups();

        @Override
        Stream<? extends AttributeType.Long> getSubs();

        @Override
        Stream<? extends Attribute.Long> getInstances();

        Attribute.Long put(long value);

        Attribute.Long put(long value, boolean isInferred);

        Attribute.Long get(long value);
    }

    interface Double extends AttributeType {

        @Override
        AttributeType.Double getSup();

        @Override
        Stream<? extends AttributeType.Double> getSups();

        @Override
        Stream<? extends AttributeType.Double> getSubs();

        @Override
        Stream<? extends Attribute.Double> getInstances();

        Attribute.Double put(double value);

        Attribute.Double put(double value, boolean isInferred);

        Attribute.Double get(double value);
    }

    interface String extends AttributeType {

        @Override
        AttributeType.String getSup();

        @Override
        Stream<? extends AttributeType.String> getSups();

        @Override
        Stream<? extends AttributeType.String> getSubs();

        @Override
        Stream<? extends Attribute.String> getInstances();

        void regex(java.lang.String regex);

        java.lang.String regex();

        Attribute.String put(java.lang.String value);

        Attribute.String put(java.lang.String value, boolean isInferred);

        Attribute.String get(java.lang.String value);
    }

    interface DateTime extends AttributeType {

        @Override
        AttributeType.DateTime getSup();

        @Override
        Stream<? extends AttributeType.DateTime> getSups();

        @Override
        Stream<? extends AttributeType.DateTime> getSubs();

        @Override
        Stream<? extends Attribute.DateTime> getInstances();

        Attribute.DateTime put(LocalDateTime value);

        Attribute.DateTime put(LocalDateTime value, boolean isInferred);

        Attribute.DateTime get(LocalDateTime value);
    }
}
