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

package com.vaticle.typedb.core.concept.type;

import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Seekable;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typeql.lang.common.TypeQLArg;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public interface AttributeType extends ThingType {

    @Override
    Seekable<? extends AttributeType, Order.Asc> getSubtypes();

    @Override
    Seekable<? extends AttributeType, Order.Asc> getSubtypesExplicit();

    @Override
    Seekable<? extends Attribute, Order.Asc> getInstances();

    @Override
    Seekable<? extends Attribute, Order.Asc> getInstancesExplicit();

    void setSupertype(AttributeType superType);

    boolean isKeyable();

    ValueType getValueType();

    Seekable<? extends ThingType, Order.Asc> getOwners();

    Seekable<? extends ThingType, Order.Asc> getOwners(boolean onlyKey);

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
        private Set<ValueType> comparables;
        private Set<ValueType> assignables;

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

        public static ValueType of(TypeQLArg.ValueType valueType) {
            for (ValueType vt : ValueType.values()) {
                if (Objects.equals(vt.encoding.typeQLValueType(), valueType)) {
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
            if (comparables == null) comparables = iterate(encoding.comparables()).map(ValueType::of).toSet();
            return comparables;
        }

        public Set<ValueType> assignables() {
            if (assignables == null) assignables = iterate(encoding.assignables()).map(ValueType::of).toSet();
            return assignables;
        }

    }

    interface Boolean extends AttributeType {

        @Override
        Seekable<? extends AttributeType.Boolean, Order.Asc> getSubtypes();

        @Override
        Seekable<? extends AttributeType.Boolean, Order.Asc> getSubtypesExplicit();

        @Override
        Seekable<? extends Attribute.Boolean, Order.Asc> getInstances();

        @Override
        Seekable<? extends Attribute.Boolean, Order.Asc> getInstancesExplicit();

        Attribute.Boolean put(boolean value);

        Attribute.Boolean put(boolean value, boolean isInferred);

        Attribute.Boolean get(boolean value);
    }

    interface Long extends AttributeType {

        @Override
        Seekable<? extends AttributeType.Long, Order.Asc> getSubtypes();

        @Override
        Seekable<? extends AttributeType.Long, Order.Asc> getSubtypesExplicit();

        @Override
        Seekable<? extends Attribute.Long, Order.Asc> getInstances();

        @Override
        Seekable<? extends Attribute.Long, Order.Asc> getInstancesExplicit();

        Attribute.Long put(long value);

        Attribute.Long put(long value, boolean isInferred);

        Attribute.Long get(long value);
    }

    interface Double extends AttributeType {

        @Override
        Seekable<? extends AttributeType.Double, Order.Asc> getSubtypes();

        @Override
        Seekable<? extends AttributeType.Double, Order.Asc> getSubtypesExplicit();

        @Override
        Seekable<? extends Attribute.Double, Order.Asc> getInstances();

        @Override
        Seekable<? extends Attribute.Double, Order.Asc> getInstancesExplicit();

        Attribute.Double put(double value);

        Attribute.Double put(double value, boolean isInferred);

        Attribute.Double get(double value);
    }

    interface String extends AttributeType {

        @Override
        Seekable<? extends AttributeType.String, Order.Asc> getSubtypes();

        @Override
        Seekable<? extends AttributeType.String, Order.Asc> getSubtypesExplicit();

        @Override
        Seekable<? extends Attribute.String, Order.Asc> getInstances();

        @Override
        Seekable<? extends Attribute.String, Order.Asc> getInstancesExplicit();

        void setRegex(Pattern regex);

        void unsetRegex();

        Pattern getRegex();

        Attribute.String put(java.lang.String value);

        Attribute.String put(java.lang.String value, boolean isInferred);

        Attribute.String get(java.lang.String value);
    }

    interface DateTime extends AttributeType {

        @Override
        Seekable<? extends AttributeType.DateTime, Order.Asc> getSubtypes();

        @Override
        Seekable<? extends AttributeType.DateTime, Order.Asc> getSubtypesExplicit();

        @Override
        Seekable<? extends Attribute.DateTime, Order.Asc> getInstances();

        @Override
        Seekable<? extends Attribute.DateTime, Order.Asc> getInstancesExplicit();

        Attribute.DateTime put(LocalDateTime value);

        Attribute.DateTime put(LocalDateTime value, boolean isInferred);

        Attribute.DateTime get(LocalDateTime value);
    }
}
