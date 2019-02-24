/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.server.kb.concept;

import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Thing;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.stream.Stream;

/**
 * Represent a literal resource in the graph.
 * Acts as an Thing when relating to other instances except it has the added functionality of:
 * 1. It is unique to its AttributeType based on it's value.
 * 2. It has a AttributeType.DataType associated with it which constrains the allowed values.
 *
 * @param <D> The data type of this resource type.
 *            Supported Types include: String, Long, Double, and Boolean
 */
public class AttributeImpl<D> extends ThingImpl<Attribute<D>, AttributeType<D>> implements Attribute<D> {
    private AttributeImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    private AttributeImpl(VertexElement vertexElement, AttributeType<D> type, D value) {
        super(vertexElement, type);
        setValue(value);
    }

    public static <D> AttributeImpl<D> get(VertexElement vertexElement) {
        return new AttributeImpl<>(vertexElement);
    }

    public static <D> AttributeImpl<D> create(VertexElement vertexElement, AttributeType<D> type, D value) {
        if (!type.dataType().getValueClass().isInstance(value)) {
            throw TransactionException.invalidAttributeValue(value, type.dataType());
        }

        AttributeImpl<D> attribute = new AttributeImpl<>(vertexElement, type, value);

        //Generate the index again. Faster than reading
        String index = Schema.generateAttributeIndex(type.label(), value.toString());
        vertexElement.property(Schema.VertexProperty.INDEX, index);

        //Track the attribute by index
        vertexElement.tx().cache().addNewAttribute(index, attribute.id());
        return attribute;
    }

    public static AttributeImpl from(Attribute attribute) {
        return (AttributeImpl) attribute;
    }

    /**
     * @return The data type of this Attribute's AttributeType.
     */
    @Override
    public AttributeType.DataType<D> dataType() {
        return type().dataType();
    }

    /**
     * @return The list of all Instances which possess this resource
     */
    @Override
    public Stream<Thing> owners() {
        //Get Owner via implicit structure
        Stream<Thing> implicitOwners = getShortcutNeighbours(false);
        //Get owners via edges
        Stream<Thing> edgeOwners = neighbours(Direction.IN, Schema.EdgeLabel.ATTRIBUTE);

        return Stream.concat(implicitOwners, edgeOwners);
    }

    /**
     * @param value The value to store on the resource
     */
    private void setValue(D value) {
        Object valueToPersist = Serialise.of(dataType()).serialised(value);
        Schema.VertexProperty property = Schema.VertexProperty.ofDataType(dataType());
        vertex().propertyImmutable(property, valueToPersist, vertex().property(property));
    }

    /**
     * @return The value casted to the correct type
     */
    @Override
    public D value() {
        return Serialise.of(dataType()).deserialised(
                vertex().property(Schema.VertexProperty.ofDataType(dataType()))
        );
    }

    @Override
    public String innerToString() {
        return super.innerToString() + "- Value [" + value() + "] ";
    }
}
