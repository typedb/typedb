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

import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Resource;
import ai.grakn.exception.InvalidConceptValueException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * A concept which represents a resource.
 * @param <D> The data type of this resource. Supported Types include: String, Long, Double, and Boolean
 */
class ResourceImpl<D> extends InstanceImpl<Resource<D>, ResourceType<D>> implements Resource<D> {
    ResourceImpl(Vertex v, ResourceType type, AbstractGraknGraph graknGraph, D value) {
        super(v, type, graknGraph);
        setValue(value);
    }
    ResourceImpl(Vertex v, ResourceType type, AbstractGraknGraph graknGraph) {
        super(v, type, graknGraph);
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
        Set<Instance> owners = new HashSet<>();
        this.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).forEach(concept -> {
            if(!concept.getBaseType().equals(Schema.BaseType.RESOURCE.name()))
                owners.add(concept.asInstance());
        });
        return owners;
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
            if(regex != null){
                if(!Pattern.matches(regex, (String) value)){
                    throw new InvalidConceptValueException(ErrorMessage.REGEX_INSTANCE_FAILURE.getMessage(regex, toString()));
                }
            }

            Schema.ConceptProperty property = dataType().getConceptProperty();
            setImmutableProperty(property, castValue(value), getProperty(property), Function.identity());

            return setUniqueProperty(Schema.ConceptProperty.INDEX, generateResourceIndex(type(), value.toString()));
        } catch (ClassCastException e) {
            throw new InvalidConceptValueException(ErrorMessage.INVALID_DATATYPE.getMessage(value, dataType().getName()));
        }
    }

    /**
     *
     * @param resourceType it's resource type
     * @param value The value of the resource
     * @return A unique id for the resource
     */
    public static String generateResourceIndex(ResourceType resourceType, String value){
        return Schema.BaseType.RESOURCE.name() + "-" + resourceType.getName() + "-" + value;
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
        return getProperty(dataType().getConceptProperty());
    }

    @Override
    public String toString(){
        return super.toString() + "- Value [" + getValue() + "] ";
    }
}
