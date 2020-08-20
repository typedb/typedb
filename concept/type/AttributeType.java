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
import graql.lang.common.GraqlArg;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.stream.Stream;

public interface AttributeType extends ThingType {

    @Override
    default AttributeType asAttributeType() { return this; }

    @Override
    AttributeType getSupertype();

    @Override
    Stream<? extends AttributeType> getSupertypes();

    @Override
    Stream<? extends AttributeType> getSubtypes();

    @Override
    Stream<? extends Attribute> getInstances();

    void setSupertype(AttributeType superType);

    boolean isKeyable();

    ValueType getValueType();

    Stream<? extends ThingType> getOwners();

    Stream<? extends ThingType> getOwners(boolean onlyKey);

    AttributeType asObject();

    AttributeType.Boolean asBoolean();

    AttributeType.Long asLong();

    AttributeType.Double asDouble();

    AttributeType.String asString();

    AttributeType.DateTime asDateTime();

    enum ValueType {
        OBJECT(Schema.ValueType.OBJECT, null),
        BOOLEAN(Schema.ValueType.BOOLEAN, GraqlArg.ValueType.BOOLEAN),
        LONG(Schema.ValueType.LONG, GraqlArg.ValueType.LONG),
        DOUBLE(Schema.ValueType.DOUBLE, GraqlArg.ValueType.DOUBLE),
        STRING(Schema.ValueType.STRING, GraqlArg.ValueType.STRING),
        DATETIME(Schema.ValueType.DATETIME, GraqlArg.ValueType.DATETIME);

        private final Class<?> valueClass;
        private final boolean isWritable;
        private final boolean isKeyable;
        private final GraqlArg.ValueType graqlValueType;

        ValueType(Schema.ValueType schemaValueType, GraqlArg.ValueType graqlValueType) {
            this.valueClass = schemaValueType.valueClass();
            this.isWritable = schemaValueType.isWritable();
            this.isKeyable = schemaValueType.isKeyable();
            this.graqlValueType = graqlValueType;
        }

        public static ValueType of(Class<?> valueClass) {
            for (ValueType vt : ValueType.values()) {
                if (vt.valueClass.equals(valueClass)) {
                    return vt;
                }
            }
            return null;
        }

        public static ValueType of(GraqlArg.ValueType valueType) {
            for (ValueType vt : ValueType.values()) {
                if (Objects.equals(vt.graqlValueType, valueType)) {
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

        public boolean isKeyable() {
            return isKeyable;
        }
    }

    interface Boolean extends AttributeType {

        @Override
        AttributeType.Boolean getSupertype();

        @Override
        Stream<? extends AttributeType.Boolean> getSupertypes();

        @Override
        Stream<? extends AttributeType.Boolean> getSubtypes();

        @Override
        Stream<? extends Attribute.Boolean> getInstances();

        Attribute.Boolean put(boolean value);

        Attribute.Boolean put(boolean value, boolean isInferred);

        Attribute.Boolean get(boolean value);
    }

    interface Long extends AttributeType {

        @Override
        AttributeType.Long getSupertype();

        @Override
        Stream<? extends AttributeType.Long> getSupertypes();

        @Override
        Stream<? extends AttributeType.Long> getSubtypes();

        @Override
        Stream<? extends Attribute.Long> getInstances();

        Attribute.Long put(long value);

        Attribute.Long put(long value, boolean isInferred);

        Attribute.Long get(long value);
    }

    interface Double extends AttributeType {

        @Override
        AttributeType.Double getSupertype();

        @Override
        Stream<? extends AttributeType.Double> getSupertypes();

        @Override
        Stream<? extends AttributeType.Double> getSubtypes();

        @Override
        Stream<? extends Attribute.Double> getInstances();

        Attribute.Double put(double value);

        Attribute.Double put(double value, boolean isInferred);

        Attribute.Double get(double value);
    }

    interface String extends AttributeType {

        @Override
        AttributeType.String getSupertype();

        @Override
        Stream<? extends AttributeType.String> getSupertypes();

        @Override
        Stream<? extends AttributeType.String> getSubtypes();

        @Override
        Stream<? extends Attribute.String> getInstances();

        void setRegex(java.lang.String regex);

        void unsetRegex();

        java.lang.String getRegex();

        Attribute.String put(java.lang.String value);

        Attribute.String put(java.lang.String value, boolean isInferred);

        Attribute.String get(java.lang.String value);
    }

    interface DateTime extends AttributeType {

        @Override
        AttributeType.DateTime getSupertype();

        @Override
        Stream<? extends AttributeType.DateTime> getSupertypes();

        @Override
        Stream<? extends AttributeType.DateTime> getSubtypes();

        @Override
        Stream<? extends Attribute.DateTime> getInstances();

        Attribute.DateTime put(LocalDateTime value);

        Attribute.DateTime put(LocalDateTime value, boolean isInferred);

        Attribute.DateTime get(LocalDateTime value);
    }
}
