/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

import grakn.core.concept.Label;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.Role;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.structure.VertexElement;
import grakn.core.server.session.ConceptObserver;
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
    private AttributeImpl(VertexElement vertexElement, ConceptManager conceptManager, ConceptObserver conceptObserver) {
        super(vertexElement, conceptManager, conceptObserver);
    }

    private AttributeImpl(VertexElement vertexElement, AttributeType<D> type, D value,
                          ConceptManager conceptManager, ConceptObserver conceptObserver) {
        super(vertexElement, type, conceptManager, conceptObserver);
        setValue(value);
        setUniqueIndexedValue(vertexElement, type, value);
    }

    public static <D> AttributeImpl<D> get(VertexElement vertexElement, ConceptManager conceptManager, ConceptObserver conceptObserver) {
        return new AttributeImpl<>(vertexElement, conceptManager, conceptObserver);
    }

    public static <D> AttributeImpl<D> create(VertexElement vertexElement, AttributeType<D> type, D value,
                                              ConceptManager conceptManager, ConceptObserver conceptObserver) {
        D converted;
        try {
            converted = ValueConverter.of(type.dataType()).convert(value);
        } catch (ClassCastException e){
            throw TransactionException.invalidAttributeValue(value, type.dataType());
        }

        return new AttributeImpl<>(vertexElement, type, converted, conceptManager, conceptObserver);
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
     * @param value The value to store on the resource in its native format
     */
    private void setValue(D value) {
        Object valueToPersist = Serialiser.of(dataType()).serialise(value);
        Schema.VertexProperty property = Schema.VertexProperty.ofDataType(dataType());
        vertex().propertyImmutable(property, valueToPersist, vertex().property(property));
    }

    /**
     * Set the unique combination of type and value to an indexed Janus property for lookups
     * @param vertexElement
     * @param type
     * @param value
     */
    private void setUniqueIndexedValue(VertexElement vertexElement, AttributeType<D> type, D value) {
        //Generate the index again. Faster than reading
        String index = Schema.generateAttributeIndex(type.label(), value.toString());
        vertexElement.property(Schema.VertexProperty.INDEX, index);
    }

    /**
     * @return The value casted to the correct type
     */
    @Override
    public D value() {
        return Serialiser.of(dataType()).deserialise(
                vertex().property(Schema.VertexProperty.ofDataType(dataType()))
        );
    }

    @Override
    public String innerToString() {
        return super.innerToString() + "- Value [" + value() + "] ";
    }

    @Override
    public Stream<Thing> getDependentConcepts() {
        Label typeLabel = type().label();
        Role hasRole = conceptManager.getRole(Schema.ImplicitType.HAS_VALUE.getLabel(typeLabel).getValue());
        Role keyRole = conceptManager.getRole(Schema.ImplicitType.KEY_VALUE.getLabel(typeLabel).getValue());
        Stream<Thing> conceptStream = Stream.of(this);
        if (hasRole != null) conceptStream = Stream.concat(conceptStream, relations(hasRole));
        if (keyRole != null) conceptStream = Stream.concat(conceptStream, relations(keyRole));
        return conceptStream;
    }
}
