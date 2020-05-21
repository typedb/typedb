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

import hypergraph.concept.thing.Attribute;
import hypergraph.concept.type.impl.AttributeTypeImpl;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.ThingVertex;

public abstract class AttributeImpl<VALUE> extends ThingImpl implements Attribute {

    protected final ThingVertex.Attribute<VALUE> attributeVertex;

    public AttributeImpl(ThingVertex.Attribute<VALUE> vertex) {
        super(vertex);
        this.attributeVertex = vertex;
    }

    @Override
    public AttributeTypeImpl type() {
        return AttributeTypeImpl.of(vertex.typeVertex());
    }

    @Override
    public AttributeImpl has(Attribute attribute) {
        return null; //TODO
    }

    public abstract VALUE value();

    public static class Boolean extends AttributeImpl<java.lang.Boolean> implements Attribute.Boolean {

        public Boolean(ThingVertex.Attribute<java.lang.Boolean> vertex) {
            super(vertex);
            assert vertex.typeVertex().valueType().equals(Schema.ValueType.BOOLEAN);
        }

        @Override
        public java.lang.Boolean value() {
            return attributeVertex.value();
        }
    }

    public static class Long extends AttributeImpl<java.lang.Long> implements Attribute.Long {

        public Long(ThingVertex.Attribute<java.lang.Long> vertex) {
            super(vertex);
            assert vertex.typeVertex().valueType().equals(Schema.ValueType.LONG);
        }

        @Override
        public java.lang.Long value() {
            return attributeVertex.value();
        }
    }

    public static class Double extends AttributeImpl<java.lang.Double> implements Attribute.Double {

        public Double(ThingVertex.Attribute<java.lang.Double> vertex) {
            super(vertex);
            assert vertex.typeVertex().valueType().equals(Schema.ValueType.DOUBLE);
        }

        @Override
        public java.lang.Double value() {
            return attributeVertex.value();
        }
    }

    public static class String extends AttributeImpl<java.lang.String> implements Attribute.String {

        public String(ThingVertex.Attribute<java.lang.String> vertex) {
            super(vertex);
            assert vertex.typeVertex().valueType().equals(Schema.ValueType.STRING);
        }

        @Override
        public java.lang.String value() {
            return attributeVertex.value();
        }
    }

    public static class DateTime extends AttributeImpl<java.time.LocalDateTime> implements Attribute.DateTime {

        public DateTime(ThingVertex.Attribute<java.time.LocalDateTime> vertex) {
            super(vertex);
            assert vertex.typeVertex().valueType().equals(Schema.ValueType.DATETIME);
        }

        @Override
        public java.time.LocalDateTime value() {
            return attributeVertex.value();
        }
    }
}
