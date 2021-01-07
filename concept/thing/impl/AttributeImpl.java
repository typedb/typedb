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

package grakn.core.concept.thing.impl;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.type.ThingType;
import grakn.core.concept.type.impl.AttributeTypeImpl;
import grakn.core.concept.type.impl.ThingTypeImpl;
import grakn.core.graph.iid.PrefixIID;
import grakn.core.graph.vertex.AttributeVertex;

import java.time.LocalDateTime;
import java.util.stream.Stream;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_CASTING;
import static grakn.core.graph.util.Encoding.Graph.Edge.Thing.HAS;
import static grakn.core.graph.util.Encoding.Graph.ValueType.BOOLEAN;
import static grakn.core.graph.util.Encoding.Graph.ValueType.DATETIME;
import static grakn.core.graph.util.Encoding.Graph.ValueType.DOUBLE;
import static grakn.core.graph.util.Encoding.Graph.ValueType.LONG;
import static grakn.core.graph.util.Encoding.Graph.ValueType.STRING;

public abstract class AttributeImpl<VALUE> extends ThingImpl implements Attribute {

    final AttributeVertex<VALUE> attributeVertex;

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
    public AttributeTypeImpl getType() {
        return AttributeTypeImpl.of(vertex.graphs(), vertex.type());
    }

    @Override
    public Stream<ThingImpl> getOwners() {
        return vertex.ins().edge(HAS).from().stream().map(ThingImpl::of);
    }

    @Override
    public Stream<ThingImpl> getOwners(ThingType ownerType) {
        return ownerType.getSubtypes().map(ot -> ((ThingTypeImpl) ot).vertex).flatMap(
                v -> vertex.ins().edge(HAS, PrefixIID.of(v.encoding().instance()), v.iid()).from().stream()
        ).map(ThingImpl::of);
    }

    @Override
    public void validate() {
        super.validate();
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
        throw exception(GraknException.of(INVALID_THING_CASTING, className(this.getClass()), className(Attribute.Boolean.class)));
    }

    @Override
    public AttributeImpl.Long asLong() {
        throw exception(GraknException.of(INVALID_THING_CASTING, className(this.getClass()), className(Attribute.Long.class)));
    }

    @Override
    public AttributeImpl.Double asDouble() {
        throw exception(GraknException.of(INVALID_THING_CASTING, className(this.getClass()), className(Attribute.Double.class)));
    }

    @Override
    public AttributeImpl.String asString() {
        throw exception(GraknException.of(INVALID_THING_CASTING, className(this.getClass()), className(Attribute.String.class)));
    }

    @Override
    public AttributeImpl.DateTime asDateTime() {
        throw exception(GraknException.of(INVALID_THING_CASTING, className(this.getClass()), className(Attribute.DateTime.class)));
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
