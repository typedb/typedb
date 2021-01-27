/*
 * Copyright (C) 2021 Grakn Labs
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
import grakn.core.graph.common.Encoding;
import graql.lang.common.GraqlArg;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static grakn.core.common.iterator.Iterators.iterate;

public interface AttributeType extends ThingType {

    @Override
    Stream<? extends AttributeType> getSubtypes();

    @Override
    Stream<? extends AttributeType> getSubtypesExplicit();

    @Override
    Stream<? extends Attribute> getInstances();

    void setSupertype(AttributeType superType);

    boolean isKeyable();

    ValueType getValueType();

    Stream<? extends ThingType> getOwners();

    Stream<? extends ThingType> getOwners(boolean onlyKey);

    boolean isBoolean();

    boolean isLong();

    boolean isDouble();

    boolean isString();

    boolean isDateTime();

    AttributeType.Boolean asBoolean();

    AttributeType.Long asLong();

    AttributeType.Double asDouble();

    AttributeType.String asString();

    AttributeType.DateTime asDateTime();

    enum ValueType {
        OBJECT(Encoding.ValueType.OBJECT),
        BOOLEAN(Encoding.ValueType.BOOLEAN),
        LONG(Encoding.ValueType.LONG),
        DOUBLE(Encoding.ValueType.DOUBLE),
        STRING(Encoding.ValueType.STRING),
        DATETIME(Encoding.ValueType.DATETIME);

        private final Encoding.ValueType encoding;

        ValueType(Encoding.ValueType encoding) {
            this.encoding = encoding;
        }

        public static ValueType of(Class<?> valueClass) {
            for (ValueType vt : ValueType.values()) {
                if (vt.encoding.valueClass().equals(valueClass)) {
                    return vt;
                }
            }
            return null;
        }

        public static ValueType of(GraqlArg.ValueType valueType) {
            for (ValueType vt : ValueType.values()) {
                if (Objects.equals(vt.encoding.graqlValueType(), valueType)) {
                    return vt;
                }
            }
            return null;
        }

        public static ValueType of(Encoding.ValueType encoding) {
            for (ValueType vt : ValueType.values()) {
                if (vt.encoding.equals(encoding)) {
                    return vt;
                }
            }
            return null;
        }

        public Class<?> getValueClass() {
            return encoding.valueClass();
        }

        public boolean isWritable() {
            return encoding.isWritable();
        }

        public boolean isKeyable() {
            return encoding.isKeyable();
        }

        public Set<ValueType> comparables() {
            return iterate(encoding.comparables()).map(ValueType::of).toSet();
        }
    }

    interface Boolean extends AttributeType {

        @Override
        Stream<? extends AttributeType.Boolean> getSubtypes();

        @Override
        Stream<? extends AttributeType.Boolean> getSubtypesExplicit();

        @Override
        Stream<? extends Attribute.Boolean> getInstances();

        Attribute.Boolean put(boolean value);

        Attribute.Boolean put(boolean value, boolean isInferred);

        Attribute.Boolean get(boolean value);
    }

    interface Long extends AttributeType {

        @Override
        Stream<? extends AttributeType.Long> getSubtypes();

        @Override
        Stream<? extends AttributeType.Long> getSubtypesExplicit();

        @Override
        Stream<? extends Attribute.Long> getInstances();

        Attribute.Long put(long value);

        Attribute.Long put(long value, boolean isInferred);

        Attribute.Long get(long value);
    }

    interface Double extends AttributeType {

        @Override
        Stream<? extends AttributeType.Double> getSubtypes();

        @Override
        Stream<? extends AttributeType.Double> getSubtypesExplicit();

        @Override
        Stream<? extends Attribute.Double> getInstances();

        Attribute.Double put(double value);

        Attribute.Double put(double value, boolean isInferred);

        Attribute.Double get(double value);
    }

    interface String extends AttributeType {

        @Override
        Stream<? extends AttributeType.String> getSubtypes();

        @Override
        Stream<? extends AttributeType.String> getSubtypesExplicit();

        @Override
        Stream<? extends Attribute.String> getInstances();

        void setRegex(Pattern regex);

        void unsetRegex();

        Pattern getRegex();

        Attribute.String put(java.lang.String value);

        Attribute.String put(java.lang.String value, boolean isInferred);

        Attribute.String get(java.lang.String value);
    }

    interface DateTime extends AttributeType {

        @Override
        Stream<? extends AttributeType.DateTime> getSubtypes();

        @Override
        Stream<? extends AttributeType.DateTime> getSubtypesExplicit();

        @Override
        Stream<? extends Attribute.DateTime> getInstances();

        Attribute.DateTime put(LocalDateTime value);

        Attribute.DateTime put(LocalDateTime value, boolean isInferred);

        Attribute.DateTime get(LocalDateTime value);
    }
}
