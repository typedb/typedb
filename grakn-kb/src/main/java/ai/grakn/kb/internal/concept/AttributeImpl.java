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

package ai.grakn.kb.internal.concept;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.kb.internal.structure.VertexElement;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.stream.Stream;

/**
 * <p>
 *     Represent a literal resource in the graph.
 * </p>
 *
 * <p>
 *     Acts as an {@link Thing} when relating to other instances except it has the added functionality of:
 *     1. It is unique to its {@link AttributeType} based on it's value.
 *     2. It has a {@link AttributeType.DataType} associated with it which constrains the allowed values.
 * </p>
 *
 * @author fppt
 *
 * @param <D> The data type of this resource type.
 *           Supported Types include: {@link String}, {@link Long}, {@link Double}, and {@link Boolean}
 */
public class AttributeImpl<D> extends ThingImpl<Attribute<D>, AttributeType<D>> implements Attribute<D> {
    private AttributeImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    private AttributeImpl(VertexElement vertexElement, AttributeType<D> type, Object value) {
        super(vertexElement, type);
        setValue(value);
    }

    public static <D> AttributeImpl<D> get(VertexElement vertexElement){
        return new AttributeImpl<>(vertexElement);
    }

    public static <D> AttributeImpl<D> create(VertexElement vertexElement, AttributeType<D> type, Object value) {
        Object persistenceValue = castValue(type.dataType(), value);
        AttributeImpl<D> attribute = new AttributeImpl<>(vertexElement, type, persistenceValue);

        //Generate the index again. Faster than reading
        String index = Schema.generateAttributeIndex(type.label(), value.toString());
        vertexElement.property(Schema.VertexProperty.INDEX, index);

        //Track the attribute by index
        vertexElement.tx().txCache().addNewAttribute(index, attribute.id());
        return attribute;
    }

    /**
     * This is to handle casting longs and doubles when the type allows for the data type to be a number
     * @param value The value of the resource
     * @return The value casted to the correct type
     */
    private static Object castValue(AttributeType.DataType dataType, Object value){
        try {
            if (dataType.equals(AttributeType.DataType.DOUBLE)) {
                return ((Number) value).doubleValue();
            } else if (dataType.equals(AttributeType.DataType.LONG)) {
                if (value instanceof Double) {
                    throw new ClassCastException();
                }
                return ((Number) value).longValue();
            } else if (dataType.equals(AttributeType.DataType.DATE) && (value instanceof Long)){
                return value;
            }
            else {
                return dataType.getPersistenceValue(value);
            }
        } catch (ClassCastException e) {
            throw GraknTxOperationException.invalidAttributeValue(value, dataType);
        }
    }

    /**
     *
     * @return The data type of this {@link Attribute}'s {@link AttributeType}.
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
     *
     * @param value The value to store on the resource
     */
    private void setValue(Object value) {
        Schema.VertexProperty property = dataType().getVertexProperty();
        //noinspection unchecked
        vertex().propertyImmutable(property, value, vertex().property(property));
    }

    /**
     *
     * @return The value casted to the correct type
     */
    @Override
    public D value(){
        return dataType().getValue(vertex().property(dataType().getVertexProperty()));
    }

    @Override
    public String innerToString(){
        return super.innerToString() + "- Value [" + value() + "] ";
    }

    public static AttributeImpl from(Attribute attribute){
        return (AttributeImpl) attribute;
    }
}
