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

package com.vaticle.typedb.core.concept.value.impl;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.ConceptImpl;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.value.Value;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.graph.vertex.ValueVertex;

import java.time.LocalDateTime;
import java.util.Objects;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNSUPPORTED_OPERATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_CASTING;

public abstract class ValueImpl<VALUE> extends ConceptImpl implements Value<VALUE> {

    protected final ValueVertex<VALUE> vertex;

    ValueImpl(ConceptManager conceptMgr, ValueVertex<VALUE> vertex) {
        super(conceptMgr);
        this.vertex = Objects.requireNonNull(vertex);
    }

    public static <T> ValueImpl<?> of(ConceptManager conceptMgr, ValueVertex<T> vertex) {
        if (vertex.isBoolean()) return new ValueImpl.Boolean(conceptMgr, vertex.asBoolean());
        else if (vertex.isLong()) return new ValueImpl.Long(conceptMgr, vertex.asLong());
        else if (vertex.isDouble()) return new ValueImpl.Double(conceptMgr, vertex.asDouble());
        else if (vertex.isString()) return new ValueImpl.String(conceptMgr, vertex.asString());
        else if (vertex.isDateTime()) return new ValueImpl.DateTime(conceptMgr, vertex.asDateTime());
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    public ByteArray getIID() {
        return vertex.iid().bytes();
    }

    @Override
    public VALUE value() {
        return vertex.value();
    }

    public Encoding.ValueType<VALUE> valueType() {
        return vertex.valueType();
    }

    @Override
    public boolean isDeleted() {
        return false;
    }

    @Override
    public void delete() {
        throw TypeDBException.of(UNSUPPORTED_OPERATION);
    }

    @Override
    public boolean isValue() {
        return true;
    }

    @Override
    public ValueImpl<VALUE> asValue() {
        return this;
    }

    @Override
    public TypeDBException exception(TypeDBException exception) {
        return exception;
    }

    @Override
    public final int hashCode() {
        return vertex.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ValueImpl<?> that = (ValueImpl<?>) object;
        return this.vertex.equals(that.vertex);
    }

    @Override
    public java.lang.String toString() {
        return valueType().name() + ":" + value().toString();
    }

    @Override
    public boolean isBoolean() {
        return false;
    }

    @Override
    public boolean isLong() {
        return false;
    }

    @Override
    public boolean isDouble() {
        return false;
    }

    @Override
    public boolean isString() {
        return false;
    }

    @Override
    public boolean isDateTime() {
        return false;
    }

    @Override
    public ValueImpl.Boolean asBoolean() {
        throw exception(TypeDBException.of(INVALID_THING_CASTING, className(this.getClass()), className(Boolean.class)));
    }

    @Override
    public ValueImpl.Long asLong() {
        throw exception(TypeDBException.of(INVALID_THING_CASTING, className(this.getClass()), className(Long.class)));
    }

    @Override
    public ValueImpl.Double asDouble() {
        throw exception(TypeDBException.of(INVALID_THING_CASTING, className(this.getClass()), className(Double.class)));
    }

    @Override
    public ValueImpl.String asString() {
        throw exception(TypeDBException.of(INVALID_THING_CASTING, className(this.getClass()), className(String.class)));
    }

    @Override
    public ValueImpl.DateTime asDateTime() {
        throw exception(TypeDBException.of(INVALID_THING_CASTING, className(this.getClass()), className(DateTime.class)));
    }

    public static class Boolean extends ValueImpl<java.lang.Boolean> implements Value.Boolean {

        Boolean(ConceptManager conceptMgr, ValueVertex<java.lang.Boolean> vertex) {
            super(conceptMgr, vertex);
        }

        @Override
        public boolean isBoolean() {
            return true;
        }

        @Override
        public ValueImpl.Boolean asBoolean() {
            return this;
        }
    }

    public static class Long extends ValueImpl<java.lang.Long> implements Value.Long {

        Long(ConceptManager conceptMgr, ValueVertex<java.lang.Long> vertex) {
            super(conceptMgr, vertex);
        }

        @Override
        public boolean isLong() {
            return true;
        }

        @Override
        public ValueImpl.Long asLong() {
            return this;
        }
    }

    public static class Double extends ValueImpl<java.lang.Double> implements Value.Double {


        Double(ConceptManager conceptMgr, ValueVertex<java.lang.Double> vertex) {
            super(conceptMgr, vertex);
        }

        @Override
        public boolean isDouble() {
            return true;
        }

        @Override
        public ValueImpl.Double asDouble() {
            return this;
        }
    }

    public static class String extends ValueImpl<java.lang.String> implements Value.String {


        String(ConceptManager conceptMgr, ValueVertex<java.lang.String> vertex) {
            super(conceptMgr, vertex);
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public ValueImpl.String asString() {
            return this;
        }
    }

    public static class DateTime extends ValueImpl<LocalDateTime> implements Value.DateTime {

        DateTime(ConceptManager conceptMgr, ValueVertex<LocalDateTime> vertex) {
            super(conceptMgr, vertex);
        }

        @Override
        public boolean isDateTime() {
            return true;
        }

        @Override
        public ValueImpl.DateTime asDateTime() {
            return this;
        }
    }
}
