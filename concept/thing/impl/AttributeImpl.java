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

package com.vaticle.typedb.core.concept.thing.impl;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.impl.AttributeTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.ThingTypeImpl;
import com.vaticle.typedb.core.graph.iid.PrefixIID;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;

import java.time.LocalDateTime;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_CASTING;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.HAS;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.BOOLEAN;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.DATETIME;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.DOUBLE;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.LONG;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING;

public abstract class AttributeImpl<VALUE> extends ThingImpl implements Attribute {

    AttributeVertex<VALUE> attributeVertex;

    private AttributeImpl(AttributeVertex<VALUE> vertex) {
        super(vertex);
        this.attributeVertex = vertex;
    }

    public static AttributeImpl<?> of(AttributeVertex<?> vertex) {
        switch (vertex.valueType()) {
            case BOOLEAN:
                return new AttributeImpl.Boolean(vertex.asBoolean());
            case LONG:
                return new AttributeImpl.Long(vertex.asLong());
            case DOUBLE:
                return new AttributeImpl.Double(vertex.asDouble());
            case STRING:
                return new AttributeImpl.String(vertex.asString());
            case DATETIME:
                return new AttributeImpl.DateTime(vertex.asDateTime());
            default:
                assert false;
                return null;
        }
    }

    public abstract VALUE getValue();

    @Override
    protected AttributeVertex.Write<VALUE> vertexWritable() {
        if (!attributeVertex.isWrite()) attributeVertex = attributeVertex.writable();
        return attributeVertex.asWrite();
    }

    @Override
    public AttributeTypeImpl getType() {
        return AttributeTypeImpl.of(vertex().graphs(), vertex().type());
    }

    @Override
    public FunctionalIterator<ThingImpl> getOwners() {
        return vertex().ins().edge(HAS).from().map(ThingImpl::of);
    }

    @Override
    public FunctionalIterator<ThingImpl> getOwners(ThingType ownerType) {
        return ownerType.getSubtypes().map(ot -> ((ThingTypeImpl) ot).vertex()).flatMap(
                v -> vertex().ins().edge(HAS, PrefixIID.of(v.encoding().instance()), v.iid()).from()
        ).map(ThingImpl::of);
    }

    @Override
    public boolean isAttribute() {
        return true;
    }

    @Override
    public AttributeImpl<?> asAttribute() {
        return this;
    }

    @Override
    public java.lang.String toString() {
        return vertex().encoding().name() + ":" + vertex().type().properLabel() + ":" + getValue();
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
    public AttributeImpl.Boolean asBoolean() {
        throw exception(TypeDBException.of(INVALID_THING_CASTING, className(this.getClass()), className(Attribute.Boolean.class)));
    }

    @Override
    public AttributeImpl.Long asLong() {
        throw exception(TypeDBException.of(INVALID_THING_CASTING, className(this.getClass()), className(Attribute.Long.class)));
    }

    @Override
    public AttributeImpl.Double asDouble() {
        throw exception(TypeDBException.of(INVALID_THING_CASTING, className(this.getClass()), className(Attribute.Double.class)));
    }

    @Override
    public AttributeImpl.String asString() {
        throw exception(TypeDBException.of(INVALID_THING_CASTING, className(this.getClass()), className(Attribute.String.class)));
    }

    @Override
    public AttributeImpl.DateTime asDateTime() {
        throw exception(TypeDBException.of(INVALID_THING_CASTING, className(this.getClass()), className(Attribute.DateTime.class)));
    }

    public static class Boolean extends AttributeImpl<java.lang.Boolean> implements Attribute.Boolean {

        public Boolean(AttributeVertex<java.lang.Boolean> vertex) {
            super(vertex);
            assert vertex.type().valueType().equals(BOOLEAN);
        }

        @Override
        public java.lang.Boolean getValue() {
            return attributeVertex.value();
        }

        @Override
        public boolean isBoolean() {
            return true;
        }

        @Override
        public AttributeImpl.Boolean asBoolean() {
            return this;
        }
    }

    public static class Long extends AttributeImpl<java.lang.Long> implements Attribute.Long {

        public Long(AttributeVertex<java.lang.Long> vertex) {
            super(vertex);
            assert vertex.type().valueType().equals(LONG);
        }

        @Override
        public java.lang.Long getValue() {
            return attributeVertex.value();
        }

        @Override
        public boolean isLong() {
            return true;
        }

        @Override
        public AttributeImpl.Long asLong() {
            return this;
        }
    }

    public static class Double extends AttributeImpl<java.lang.Double> implements Attribute.Double {

        public Double(AttributeVertex<java.lang.Double> vertex) {
            super(vertex);
            assert vertex.type().valueType().equals(DOUBLE);
        }

        @Override
        public java.lang.Double getValue() {
            return attributeVertex.value();
        }

        @Override
        public boolean isDouble() {
            return true;
        }

        @Override
        public AttributeImpl.Double asDouble() {
            return this;
        }
    }

    public static class String extends AttributeImpl<java.lang.String> implements Attribute.String {

        public String(AttributeVertex<java.lang.String> vertex) {
            super(vertex);
            assert vertex.type().valueType().equals(STRING);
        }

        @Override
        public java.lang.String getValue() {
            return attributeVertex.value();
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public AttributeImpl.String asString() {
            return this;
        }
    }

    public static class DateTime extends AttributeImpl<java.time.LocalDateTime> implements Attribute.DateTime {

        public DateTime(AttributeVertex<LocalDateTime> vertex) {
            super(vertex);
            assert vertex.type().valueType().equals(DATETIME);
        }

        @Override
        public java.time.LocalDateTime getValue() {
            return attributeVertex.value();
        }

        @Override
        public boolean isDateTime() {
            return true;
        }

        @Override
        public AttributeImpl.DateTime asDateTime() {
            return this;
        }
    }
}
