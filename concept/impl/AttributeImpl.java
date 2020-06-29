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

package grakn.core.concept.impl;

import grakn.core.core.AttributeSerialiser;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.manager.ConceptNotificationChannel;
import grakn.core.kb.concept.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.stream.Stream;

/**
 * Represent a literal resource in the graph.
 * Acts as an Thing when relating to other instances except it has the added functionality of:
 * 1. It is unique to its AttributeType based on it's value.
 * 2. It has a AttributeType.ValueType associated with it which constrains the allowed values.
 *
 * @param <D> The value type of this attribute type.
 *            Supported Types include: String, Long, Double, and Boolean
 */
public class AttributeImpl<D> extends ThingImpl<Attribute<D>, AttributeType<D>> implements Attribute<D> {
    public AttributeImpl(VertexElement vertexElement, ConceptManager conceptManager, ConceptNotificationChannel conceptNotificationChannel) {
        super(vertexElement, conceptManager, conceptNotificationChannel);
    }

    public static AttributeImpl from(Attribute attribute) {
        return (AttributeImpl) attribute;
    }

    /**
     * @return The value type of this Attribute's AttributeType.
     */
    @Override
    public AttributeType.ValueType<D> valueType() {
        return type().valueType();
    }

    /**
     * @return The list of all Instances which possess this resource
     */
    @Override
    public Stream<Thing> owners() {
        //Get owners via edges
        Stream<Thing> owners = neighbours(Direction.IN, Schema.EdgeLabel.ATTRIBUTE);
        return owners;
    }

    /**
     * @return The value casted to the correct type
     */
    @Override
    public D value() {
        return AttributeSerialiser.of(valueType()).deserialise(
                vertex().property(Schema.VertexProperty.ofValueType(valueType()))
        );
    }

    @Override
    public String innerToString() {
        return super.innerToString() + "- Value [" + value() + "] ";
    }
}
