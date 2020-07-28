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
import grakn.core.graph.util.Schema;
import grakn.core.graph.vertex.AttributeVertex;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;

import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.stream.Stream;

import static grakn.core.common.exception.Error.ThingRead.INVALID_THING_CASTING;
import static grakn.core.common.iterator.Iterators.apply;
import static grakn.core.common.iterator.Iterators.stream;

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

    public abstract VALUE value();

    @Override
    public AttributeTypeImpl type() {
        return AttributeTypeImpl.of(vertex.type());
    }

    @Override
    public AttributeImpl has(Attribute attribute) {
        return (AttributeImpl) super.has(attribute).asAttribute();
    }

    @Override
    public Stream<ThingImpl> owners() {
        return owners(vertex.ins().edge(Schema.Edge.Thing.HAS).from());
    }

    @Override
    public Stream<ThingImpl> owners(ThingType ownerType) {
        TypeVertex ownerVertex = ((ThingTypeImpl) ownerType).vertex;
        return owners(vertex.ins().edge(
                Schema.Edge.Thing.HAS,
                PrefixIID.of(ownerVertex.schema().instance()),
                ownerVertex.iid()
        ).from());
    }

    private Stream<ThingImpl> owners(Iterator<ThingVertex> owners) {
        return stream(apply(owners, v -> {
            switch (v.schema()) {
                case ENTITY:
                    return EntityImpl.of(v);
                case RELATION:
                    return RelationImpl.of(v);
                case ATTRIBUTE:
                    return AttributeImpl.of(v.asAttribute());
                default:
                    assert false;
                    return null;
            }
        }));
    }

    @Override
    public void validate() {
        super.validate();
    }

    @Override
    public AttributeImpl.Boolean asBoolean() {
        throw new GraknException(INVALID_THING_CASTING.message(Attribute.Boolean.class.getCanonicalName()));
    }

    @Override
    public AttributeImpl.Long asLong() {
        throw new GraknException(INVALID_THING_CASTING.message(Attribute.Long.class.getCanonicalName()));
    }

    @Override
    public AttributeImpl.Double asDouble() {
        throw new GraknException(INVALID_THING_CASTING.message(Attribute.Double.class.getCanonicalName()));
    }

    @Override
    public AttributeImpl.String asString() {
        throw new GraknException(INVALID_THING_CASTING.message(Attribute.Long.class.getCanonicalName()));
    }

    @Override
    public AttributeImpl.DateTime asDateTime() {
        throw new GraknException(INVALID_THING_CASTING.message(Attribute.DateTime.class.getCanonicalName()));
    }

    public static class Boolean extends AttributeImpl<java.lang.Boolean> implements Attribute.Boolean {

        public Boolean(AttributeVertex<java.lang.Boolean> vertex) {
            super(vertex);
            assert vertex.type().valueType().equals(Schema.ValueType.BOOLEAN);
        }

        @Override
        public java.lang.Boolean value() {
            return attributeVertex.value();
        }

        @Override
        public AttributeImpl.Boolean asBoolean() {
            return this;
        }
    }

    public static class Long extends AttributeImpl<java.lang.Long> implements Attribute.Long {

        public Long(AttributeVertex<java.lang.Long> vertex) {
            super(vertex);
            assert vertex.type().valueType().equals(Schema.ValueType.LONG);
        }

        @Override
        public java.lang.Long value() {
            return attributeVertex.value();
        }

        @Override
        public AttributeImpl.Long asLong() {
            return this;
        }
    }

    public static class Double extends AttributeImpl<java.lang.Double> implements Attribute.Double {

        public Double(AttributeVertex<java.lang.Double> vertex) {
            super(vertex);
            assert vertex.type().valueType().equals(Schema.ValueType.DOUBLE);
        }

        @Override
        public java.lang.Double value() {
            return attributeVertex.value();
        }

        @Override
        public AttributeImpl.Double asDouble() {
            return this;
        }
    }

    public static class String extends AttributeImpl<java.lang.String> implements Attribute.String {

        public String(AttributeVertex<java.lang.String> vertex) {
            super(vertex);
            assert vertex.type().valueType().equals(Schema.ValueType.STRING);
        }

        @Override
        public java.lang.String value() {
            return attributeVertex.value();
        }

        @Override
        public AttributeImpl.String asString() {
            return this;
        }
    }

    public static class DateTime extends AttributeImpl<java.time.LocalDateTime> implements Attribute.DateTime {

        public DateTime(AttributeVertex<LocalDateTime> vertex) {
            super(vertex);
            assert vertex.type().valueType().equals(Schema.ValueType.DATETIME);
        }

        @Override
        public java.time.LocalDateTime value() {
            return attributeVertex.value();
        }

        @Override
        public AttributeImpl.DateTime asDateTime() {
            return this;
        }
    }
}
