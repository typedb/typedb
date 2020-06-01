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

package hypergraph.concept.thing.impl;

import hypergraph.common.exception.HypergraphException;
import hypergraph.concept.thing.Attribute;
import hypergraph.concept.type.impl.AttributeTypeImpl;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.AttributeVertex;

import java.time.LocalDateTime;

import static hypergraph.common.exception.Error.ConceptRead.INVALID_CONCEPT_CASTING;

public abstract class AttributeImpl<VALUE> extends ThingImpl implements Attribute {

    protected final AttributeVertex<VALUE> attributeVertex;

    public AttributeImpl(AttributeVertex<VALUE> vertex) {
        super(vertex);
        this.attributeVertex = vertex;
    }

    public abstract VALUE value();

    @Override
    public AttributeTypeImpl type() {
        return AttributeTypeImpl.of(vertex.type());
    }

    @Override
    public AttributeImpl has(Attribute attribute) {
        return null; //TODO
    }

    @Override
    public AttributeImpl.Boolean asBoolean() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(Attribute.Boolean.class.getCanonicalName()));
    }

    @Override
    public AttributeImpl.Long asLong() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(Attribute.Long.class.getCanonicalName()));
    }

    @Override
    public AttributeImpl.Double asDouble() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(Attribute.Double.class.getCanonicalName()));
    }

    @Override
    public AttributeImpl.String asString() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(Attribute.Long.class.getCanonicalName()));
    }

    @Override
    public AttributeImpl.DateTime asDateTime() {
        throw new HypergraphException(INVALID_CONCEPT_CASTING.format(Attribute.DateTime.class.getCanonicalName()));
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
