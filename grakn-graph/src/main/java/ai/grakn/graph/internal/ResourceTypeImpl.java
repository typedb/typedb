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
import ai.grakn.exception.InvalidConceptValueException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 *     An ontological element which models and categorises the various {@link Resource} in the graph.
 * </p>
 *
 * <p>
 *     This ontological element behaves similarly to {@link ai.grakn.concept.Type} when defining how it relates to other
 *     types. It has two additional functions to be aware of:
 *     1. It has a {@link ai.grakn.concept.ResourceType.DataType} constraining the data types of the values it's instances may take.
 *     2. Any of it's instances are unique to the type.
 *     For example if you have a ResourceType modelling month throughout the year there can only be one January.
 * </p>
 *
 * @author fppt
 *
 * @param <D> The data type of this resource type.
 *           Supported Types include: {@link String}, {@link Long}, {@link Double}, and {@link Boolean}
 */
class ResourceTypeImpl<D> extends TypeImpl<ResourceType<D>, Resource<D>> implements ResourceType<D> {
    ResourceTypeImpl(AbstractGraknGraph graknGraph, Vertex v) {
        super(graknGraph, v);
    }

    ResourceTypeImpl(AbstractGraknGraph graknGraph, Vertex v, ResourceType<D> type, DataType<D> dataType) {
        super(graknGraph, v, type);
        setImmutableProperty(Schema.ConceptProperty.DATA_TYPE, dataType, getDataType(), DataType::getName);
    }

    private ResourceTypeImpl(ResourceTypeImpl resourceType){
        //noinspection unchecked
        super(resourceType);
    }

    @Override
    public ResourceType<D> copy(){
        //noinspection unchecked
        return new ResourceTypeImpl(this);
    }

    /**
     * @param regex The regular expression which instances of this resource must conform to.
     * @return The Resource Type itself.
     */
    @Override
    public ResourceType<D> setRegex(String regex) {
        if(getDataType() == null || !getDataType().equals(DataType.STRING)){
            throw new UnsupportedOperationException(ErrorMessage.REGEX_NOT_STRING.getMessage(getLabel()));
        }

        if(regex != null) {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher;
            for (Resource<D> resource : instances()) {
                String value = (String) resource.getValue();
                matcher = pattern.matcher(value);
                if(!matcher.matches()){
                    throw new InvalidConceptValueException(ErrorMessage.REGEX_INSTANCE_FAILURE.getMessage(regex, resource.getId(), value, getLabel()));
                }
            }
        }

        return setProperty(Schema.ConceptProperty.REGEX, regex);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Resource<D> putResource(D value) {
        if(value == null) throw new InvalidConceptValueException(ErrorMessage.NULL_VALUE.getMessage("resource value"));
        return putInstance(Schema.BaseType.RESOURCE,
                () -> getResource(value), (vertex, type) ->
                getGraknGraph().getElementFactory().buildResource(vertex, type, value));
    }

    @Override
    public <V> Resource<V> getResource(V value) {
        String index = Schema.generateResourceIndex(getLabel(), value.toString());
        return getGraknGraph().getConcept(Schema.ConceptProperty.INDEX, index);
    }

    /**
     * @return The data type which instances of this resource must conform to.
     */
    //This unsafe cast is suppressed because at this stage we do not know what the type is when reading from the rootGraph.
    @SuppressWarnings({"unchecked", "SuspiciousMethodCalls"})
    @Override
    public DataType<D> getDataType() {
        return (DataType<D>) DataType.SUPPORTED_TYPES.get(getProperty(Schema.ConceptProperty.DATA_TYPE));
    }

    /**
     * @return The regular expression which instances of this resource must conform to.
     */
    @Override
    public String getRegex() {
        return getProperty(Schema.ConceptProperty.REGEX);
    }

}
