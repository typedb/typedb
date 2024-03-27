/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.graph.vertex.impl;

import com.vaticle.typedb.core.common.exception.TypeDBCheckedException;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.ValueVertex;

import java.time.LocalDateTime;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_VERTEX_CASTING;

public abstract class ValueVertexImpl<T> extends VertexImpl<VertexIID.Value<T>> implements ValueVertex<T> {

    private ValueVertexImpl(VertexIID.Value<T> iid) {
        super(iid);
    }

    public static <TYPE> ValueVertexImpl<?> of(Encoding.ValueType<TYPE> valueType, TYPE value) throws TypeDBCheckedException {
        if (valueType == Encoding.ValueType.BOOLEAN) {
            return new ValueVertexImpl.Boolean(new VertexIID.Value.Boolean((java.lang.Boolean) value));
        } else if (valueType == Encoding.ValueType.LONG) {
            return new ValueVertexImpl.Long(new VertexIID.Value.Long((java.lang.Long) value));
        } else if (valueType == Encoding.ValueType.DOUBLE) {
            return new ValueVertexImpl.Double(new VertexIID.Value.Double((java.lang.Double) value));
        } else if (valueType == Encoding.ValueType.STRING) {
            return new ValueVertexImpl.String(new VertexIID.Value.String((java.lang.String) value));
        } else if (valueType == Encoding.ValueType.DATETIME) {
            return new ValueVertexImpl.DateTime(new VertexIID.Value.DateTime((LocalDateTime) value));
        } else throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    public T value() {
        return iid().value();
    }

    @Override
    public Encoding.ValueType<T> valueType() {
        return iid().valueType();
    }

    @Override
    public Encoding.Status status() {
        return Encoding.Status.EPHEMERAL;
    }

    @Override
    public Encoding.Vertex.Value encoding() {
        return iid.encoding();
    }

    @Override
    public boolean isModified() {
        return false;
    }

    @Override
    public boolean isValue() {
        return true;
    }

    @Override
    public ValueVertexImpl<T> asValue() {
        return this;
    }

    public boolean isBoolean() {
        return false;
    }

    public boolean isLong() {
        return false;
    }

    public boolean isDouble() {
        return false;
    }

    public boolean isString() {
        return false;
    }

    public boolean isDateTime() {
        return false;
    }

    public ValueVertexImpl<java.lang.Boolean> asBoolean() {
        throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(getClass()), className(ValueVertexImpl.Boolean.class));
    }

    public ValueVertexImpl.Long asLong() {
        throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(getClass()), className(ValueVertexImpl.Long.class));
    }

    public ValueVertexImpl.Double asDouble() {
        throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(getClass()), className(ValueVertexImpl.Double.class));
    }

    public ValueVertexImpl.String asString() {
        throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(getClass()), className(ValueVertexImpl.String.class));
    }

    public ValueVertexImpl.DateTime asDateTime() {
        throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(getClass()), className(ValueVertexImpl.DateTime.class));
    }

    public static class Boolean extends ValueVertexImpl<java.lang.Boolean> {

        Boolean(VertexIID.Value<java.lang.Boolean> iid) {
            super(iid);
        }

        @Override
        public boolean isBoolean() {
            return true;
        }

        @Override
        public Boolean asBoolean() {
            return this;
        }

    }

    public static class Long extends ValueVertexImpl<java.lang.Long> {

        Long(VertexIID.Value<java.lang.Long> iid) {
            super(iid);
        }

        @Override
        public boolean isLong() {
            return true;
        }

        @Override
        public Long asLong() {
            return this;
        }

    }

    public static class Double extends ValueVertexImpl<java.lang.Double> {

        Double(VertexIID.Value<java.lang.Double> iid) {
            super(iid);
        }

        @Override
        public boolean isDouble() {
            return true;
        }

        @Override
        public Double asDouble() {
            return this;
        }

    }

    public static class String extends ValueVertexImpl<java.lang.String> {

        String(VertexIID.Value<java.lang.String> iid) {
            super(iid);
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public String asString() {
            return this;
        }

    }

    public static class DateTime extends ValueVertexImpl<LocalDateTime> {

        DateTime(VertexIID.Value<LocalDateTime> iid) {
            super(iid);
        }

        @Override
        public boolean isDateTime() {
            return true;
        }

        @Override
        public DateTime asDateTime() {
            return this;
        }

    }
}
