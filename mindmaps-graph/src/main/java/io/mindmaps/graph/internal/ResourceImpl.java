/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graph.internal;

import io.mindmaps.util.Schema;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.exception.InvalidConceptValueException;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A concept which represents a resource.
 * @param <D> The data type of this resource. Supported Types include: String, Long, Double, and Boolean
 */
class ResourceImpl<D> extends InstanceImpl<Resource<D>, ResourceType<D>> implements Resource<D> {
    ResourceImpl(Vertex v, AbstractMindmapsGraph mindmapsGraph) {
        super(v, mindmapsGraph);
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
                owners.add(getMindmapsGraph().getElementFactory().buildSpecificInstance(concept));
        });
        return owners;
    }

    /**
     *
     * @param value The value to store on the resource
     * @return The Resource itself
     */
    public Resource<D> setValue(D value) {
        try {
            ResourceType<D> resourceType = type();

            //Not checking the datatype because the regex will always be null for non strings.
            String regex = resourceType.getRegex();
            if(regex != null){
                if(!Pattern.matches(regex, (String) value)){
                    throw new InvalidConceptValueException(ErrorMessage.REGEX_INSTANCE_FAILURE.getMessage(regex, toString()));
                }
            }

            String index = generateResourceIndex(type().getId(), value.toString());
            setUniqueProperty(Schema.ConceptPropertyUnique.INDEX, index);

            return setProperty(dataType().getConceptProperty(), castValue(value));
        } catch (ClassCastException e) {
            throw new RuntimeException(ErrorMessage.INVALID_DATATYPE.getMessage(value, dataType().getName()));
        }
    }

    /**
     *
     * @param value The value of the resource
     * @return A unique id for the resource
     */
    public static String generateResourceIndex(String typeId, String value){
        return Schema.BaseType.RESOURCE.name() + "-" + typeId + "-" + value;
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
        }
        return value;
    }

    /**
     *
     * @return The value casted to the correct type
     */
    @SuppressWarnings("unchecked")
    @Override
    public D getValue(){
        return (D) getProperty(dataType().getConceptProperty());
    }

    @Override
    public String toString(){
        return super.toString() + "- Value [" + getValue() + "] ";
    }
}
