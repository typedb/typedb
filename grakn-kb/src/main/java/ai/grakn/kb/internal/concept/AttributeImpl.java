/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.kb.internal.concept;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Thing;
import ai.grakn.kb.internal.structure.VertexElement;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Iterator;
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
        return new AttributeImpl<>(vertexElement, type, value);
    }

    /**
     *
     * @return The data type of this {@link Attribute}'s {@link AttributeType}.
     */
    @Override
    public AttributeType.DataType<D> dataType() {
        return type().getDataType();
    }

    /**
     * @return The list of all Instances which posses this resource
     */
    @Override
    public Stream<Thing> ownerInstances() {
        //Get Owner via implicit structure
        Stream<Thing> implicitOwners = getShortcutNeighbours();
        //Get owners via edges
        Stream<Thing> edgeOwners = neighbours(Direction.IN, Schema.EdgeLabel.ATTRIBUTE);

        return Stream.concat(implicitOwners, edgeOwners);
    }

    @Override
    public Thing owner() {
        Iterator<Thing> owners = ownerInstances().iterator();
        if(owners.hasNext()) {
            return owners.next();
        } else {
            return null;
        }
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
    public D getValue(){
        return dataType().getValue(vertex().property(dataType().getVertexProperty()));
    }

    @Override
    public String innerToString(){
        return super.innerToString() + "- Value [" + getValue() + "] ";
    }

    public static AttributeImpl from(Attribute attribute){
        return (AttributeImpl) attribute;
    }
}
