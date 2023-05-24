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

package com.vaticle.typedb.core.concept.type;

import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.common.parameters.Concept.Existence;
import com.vaticle.typedb.core.common.parameters.Concept.Transitivity;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typeql.lang.common.TypeQLArg;
import com.vaticle.typeql.lang.common.TypeQLToken;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public interface AttributeType extends ThingType {

    @Override
    AttributeType getSupertype();

    @Override
    Forwardable<? extends AttributeType, Order.Asc> getSupertypes();

    @Override
    Forwardable<? extends AttributeType, Order.Asc> getSubtypes();

    @Override
    Forwardable<? extends AttributeType, Order.Asc> getSubtypes(Transitivity transitivity);

    @Override
    Forwardable<? extends Attribute, Order.Asc> getInstances();

    @Override
    Forwardable<? extends Attribute, Order.Asc> getInstances(Transitivity transitivity);

    void setSupertype(AttributeType superType);

    ValueType getValueType();

    Forwardable<? extends ThingType, Order.Asc> getOwners(Set<TypeQLToken.Annotation> annotations);

    Forwardable<? extends ThingType, Order.Asc> getOwners(Transitivity transitivity, Set<TypeQLToken.Annotation> annotations);

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

    // TODO: we should consider deleting this since it can be replaced 1-1 with Encoding.ValueType
    enum ValueType {
        OBJECT(Encoding.ValueType.OBJECT),
        BOOLEAN(Encoding.ValueType.BOOLEAN),
        LONG(Encoding.ValueType.LONG),
        DOUBLE(Encoding.ValueType.DOUBLE),
        STRING(Encoding.ValueType.STRING),
        DATETIME(Encoding.ValueType.DATETIME);

        private final Encoding.ValueType<?> encoding;
        private Set<ValueType> comparables;
        private Set<ValueType> assignables;

        ValueType(Encoding.ValueType<?> encoding) {
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

        public static ValueType of(Encoding.ValueType<?> encoding) {
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

        public boolean hasExactEquality() {
            return encoding.hasExactEquality();
        }

        public Set<ValueType> comparables() {
            if (comparables == null) comparables = iterate(encoding.comparables()).map(ValueType::of).toSet();
            return comparables;
        }

        public Set<ValueType> assignables() {
            if (assignables == null) assignables = iterate(encoding.assignables()).map(ValueType::of).toSet();
            return assignables;
        }

        @Nullable
        public java.lang.String syntax() {
            if (encoding.typeQLValueType() != null) return encoding.typeQLValueType().toString();
            else return null;
        }
    }

    interface Boolean extends AttributeType {

        @Override
        Forwardable<? extends Boolean, Order.Asc> getSubtypes();

        @Override
        Forwardable<? extends Boolean, Order.Asc> getSubtypes(Transitivity transitivity);

        @Override
        Forwardable<? extends Attribute.Boolean, Order.Asc> getInstances();

        @Override
        Forwardable<? extends Attribute.Boolean, Order.Asc> getInstances(Transitivity transitivity);

        Attribute.Boolean put(boolean value);

        Attribute.Boolean put(boolean value, Existence existence);

        Attribute.Boolean get(boolean value);
    }

    interface Long extends AttributeType {

        @Override
        Forwardable<? extends Long, Order.Asc> getSubtypes();

        @Override
        Forwardable<? extends Long, Order.Asc> getSubtypes(Transitivity transitivity);

        @Override
        Forwardable<? extends Attribute.Long, Order.Asc> getInstances();

        @Override
        Forwardable<? extends Attribute.Long, Order.Asc> getInstances(Transitivity transitivity);

        Attribute.Long put(long value);

        Attribute.Long put(long value, Existence existence);

        Attribute.Long get(long value);
    }

    interface Double extends AttributeType {

        @Override
        Forwardable<? extends Double, Order.Asc> getSubtypes();

        @Override
        Forwardable<? extends Double, Order.Asc> getSubtypes(Transitivity transitivity);

        @Override
        Forwardable<? extends Attribute.Double, Order.Asc> getInstances();

        @Override
        Forwardable<? extends Attribute.Double, Order.Asc> getInstances(Transitivity transitivity);

        Attribute.Double put(double value);

        Attribute.Double put(double value, Existence existence);

        Attribute.Double get(double value);
    }

    interface String extends AttributeType {

        @Override
        Forwardable<? extends String, Order.Asc> getSubtypes();

        @Override
        Forwardable<? extends String, Order.Asc> getSubtypes(Transitivity transitivity);

        @Override
        Forwardable<? extends Attribute.String, Order.Asc> getInstances();

        @Override
        Forwardable<? extends Attribute.String, Order.Asc> getInstances(Transitivity transitivity);

        void setRegex(Pattern regex);

        void unsetRegex();

        Pattern getRegex();

        Attribute.String put(java.lang.String value);

        Attribute.String put(java.lang.String value, Existence existence);

        Attribute.String get(java.lang.String value);
    }

    interface DateTime extends AttributeType {

        @Override
        Forwardable<? extends DateTime, Order.Asc> getSubtypes();

        @Override
        Forwardable<? extends DateTime, Order.Asc> getSubtypes(Transitivity transitivity);

        @Override
        Forwardable<? extends Attribute.DateTime, Order.Asc> getInstances();

        @Override
        Forwardable<? extends Attribute.DateTime, Order.Asc> getInstances(Transitivity transitivity);

        Attribute.DateTime put(LocalDateTime value);

        Attribute.DateTime put(LocalDateTime value, Existence existence);

        Attribute.DateTime get(LocalDateTime value);
    }
}
