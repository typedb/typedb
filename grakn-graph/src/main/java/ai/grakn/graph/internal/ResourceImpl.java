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

import ai.grakn.concept.Instance;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.exception.InvalidConceptValueException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ai.grakn.util.Schema.generateResourceIndex;

/**
 * <p>
 *     Represent a literal resource in the graph.
 * </p>
 *
 * <p>
 *     Acts as an {@link Instance} when relating to other instances except it has the added functionality of:
 *     1. It is unique to its {@link ResourceType} based on it's value.
 *     2. It has a {@link ai.grakn.concept.ResourceType.DataType} associated with it which constrains the allowed values.
 * </p>
 *
 * @author fppt
 *
 * @param <D> The data type of this resource type.
 *           Supported Types include: {@link String}, {@link Long}, {@link Double}, and {@link Boolean}
 */
class ResourceImpl<D> extends InstanceImpl<Resource<D>, ResourceType<D>> implements Resource<D> {
    ResourceImpl(AbstractGraknGraph graknGraph, Vertex v) {
        super(graknGraph, v);
    }

    ResourceImpl(AbstractGraknGraph graknGraph, Vertex v, ResourceType<D> type, D value) {
        super(graknGraph, v, type);
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
    public Collection<Instance> ownerInstances() {
        return getShortcutNeighbours().stream().
                filter(concept -> !concept.isResource()).
                collect(Collectors.toSet());
    }

    @Override
    public Instance owner() {
        Collection<Instance> owners = ownerInstances();
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
            ResourceType<D> resourceType = type();

            //Not checking the datatype because the regex will always be null for non strings.
            String regex = resourceType.getRegex();
            if (regex != null && !Pattern.matches(regex, (String) value)) {
                throw new InvalidConceptValueException(ErrorMessage.REGEX_INSTANCE_FAILURE.getMessage(regex, getId(), value, type().getLabel()));
            }

            Schema.ConceptProperty property = dataType().getConceptProperty();

            //noinspection unchecked
            setImmutableProperty(property, castValue(value), getProperty(property), (v) -> resourceType.getDataType().getPersistenceValue((D) v));

            return setUniqueProperty(Schema.ConceptProperty.INDEX, generateResourceIndex(type().getLabel(), value.toString()));
        } catch (ClassCastException e) {
            throw new InvalidConceptValueException(ErrorMessage.INVALID_DATATYPE.getMessage(value, dataType().getName()));
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
        return dataType().getValue(getProperty(dataType().getConceptProperty()));
    }

    @Override
    public String innerToString(){
        return super.innerToString() + "- Value [" + getValue() + "] ";
    }
}
