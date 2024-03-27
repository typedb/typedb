/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concept.value.impl;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBCheckedException;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.ConceptImpl;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.value.Value;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.graph.vertex.ValueVertex;
import com.vaticle.typedb.core.graph.vertex.impl.ValueVertexImpl;

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

    public static ValueImpl.Boolean of(ConceptManager conceptMgr, boolean value) throws TypeDBCheckedException {
        return new Boolean(conceptMgr, ValueVertexImpl.of(Encoding.ValueType.BOOLEAN, value).asBoolean());
    }

    public static ValueImpl.Long of(ConceptManager conceptMgr, long value) throws TypeDBCheckedException {
        return new Long(conceptMgr, ValueVertexImpl.of(Encoding.ValueType.LONG, value).asLong());
    }

    public static ValueImpl.Double of(ConceptManager conceptMgr, double value) throws TypeDBCheckedException {
        return new Double(conceptMgr, ValueVertexImpl.of(Encoding.ValueType.DOUBLE, value).asDouble());
    }

    public static ValueImpl.String of(ConceptManager conceptMgr, java.lang.String value) throws TypeDBCheckedException {
        return new String(conceptMgr, ValueVertexImpl.of(Encoding.ValueType.STRING, value).asString());
    }

    public static ValueImpl.DateTime of(ConceptManager conceptMgr, LocalDateTime value) throws TypeDBCheckedException {
        return new DateTime(conceptMgr, ValueVertexImpl.of(Encoding.ValueType.DATETIME, value).asDateTime());
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

        @Override
        public java.lang.String toString() {
            return valueType().name() + ":" + DATE_TIME_FORMATTER_MILLIS.format(value());
        }
    }
}
