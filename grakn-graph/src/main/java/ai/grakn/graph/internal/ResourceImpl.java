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

package ai.grakn.graph.internal;

import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collection;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ai.grakn.util.Schema.generateResourceIndex;

/**
 * <p>
 *     Represent a literal resource in the graph.
 * </p>
 *
 * <p>
 *     Acts as an {@link Thing} when relating to other instances except it has the added functionality of:
 *     1. It is unique to its {@link ResourceType} based on it's value.
 *     2. It has a {@link ai.grakn.concept.ResourceType.DataType} associated with it which constrains the allowed values.
 * </p>
 *
 * @author fppt
 *
 * @param <D> The data type of this resource type.
 *           Supported Types include: {@link String}, {@link Long}, {@link Double}, and {@link Boolean}
 */
class ResourceImpl<D> extends ThingImpl<Resource<D>, ResourceType<D>> implements Resource<D> {
    ResourceImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    ResourceImpl(VertexElement vertexElement, ResourceType<D> type, D value) {
        super(vertexElement, type);
        setValue(value);
    }

    /**
     *
     * @return The data type of this Resource's type.
     */
    @Override
    public ResourceType.DataType<D> dataType() {
        return type().getDataType();
    }

    /**
     * @return The list of all Instances which posses this resource
     */
    @Override
    public Collection<Thing> ownerInstances() {
        //Get Owner via implicit structure
        Set<Thing> owners = getShortcutNeighbours().stream().
                filter(concept -> !concept.isResource()).
                collect(Collectors.toSet());

        //Get owners via edges
        neighbours(Direction.IN, Schema.EdgeLabel.RESOURCE).forEach(concept -> owners.add(concept.asThing()));

        return owners;
    }

    @Override
    public Thing owner() {
        Collection<Thing> owners = ownerInstances();
        if(owners.isEmpty()) {
            return null;
        } else {
            return owners.iterator().next();
        }
    }

    /**
     *
     * @param value The value to store on the resource
     * @return The Resource itself
     */
    private Resource<D> setValue(D value) {
        try {
            checkConformsToRegexes(value);

            ResourceTypeImpl<D> resourceType = (ResourceTypeImpl<D>) type();
            Schema.VertexProperty property = dataType().getVertexProperty();
            //noinspection unchecked
            vertex().propertyImmutable(property, castValue(value), vertex().property(property), (v) -> resourceType.getDataType().getPersistenceValue((D) v));
            vertex().propertyUnique(Schema.VertexProperty.INDEX, generateResourceIndex(type().getLabel(), value.toString()));

            return getThis();
        } catch (ClassCastException e) {
            throw GraphOperationException.invalidResourceValue(value, dataType());
        }
    }

    /**
     * Checks if all the regex's of the types of this resource conforms to the value provided.
     *
     * @throws GraphOperationException when the value does not conform to the regex of its types
     * @param value The value to check the regexes against.
     */
    private void checkConformsToRegexes(D value){
        //Not checking the datatype because the regex will always be null for non strings.
        for (ResourceType rt : ((ResourceTypeImpl<D>) type()).superSet()) {
            String regex = rt.getRegex();
            if (regex != null && !Pattern.matches(regex, (String) value)) {
                throw GraphOperationException.regexFailure(this, (String) value, regex);
            }
        }
    }

    /**
     * This is to handle casting longs and doubles when the type allows for the data type to be a number
     * @param value The value of the resource
     * @return The value casted to the correct type
     */
    private Object castValue(Object value){
        ResourceType.DataType<D> parentDataType = dataType();
        if(parentDataType.equals(ResourceType.DataType.DOUBLE)){
            return ((Number) value).doubleValue();
        } else if(parentDataType.equals(ResourceType.DataType.LONG)){
            if(value instanceof Double){
                throw new ClassCastException();
            }
            return ((Number) value).longValue();
        } else {
            try {
                return Class.forName(parentDataType.getName()).cast(value);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(ErrorMessage.INVALID_RESOURCE_CAST.getMessage(value, parentDataType.getName()));
            }
        }
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

    public static ResourceImpl from(Resource resource){
        return (ResourceImpl) resource;
    }
}
