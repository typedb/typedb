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

import ai.grakn.concept.Concept;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.Type;
import ai.grakn.exception.InvalidConceptValueException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Resource Type which can hold different values.
 * @param <D> The data tyoe of this resource type.
 */
class ResourceTypeImpl<D> extends TypeImpl<ResourceType<D>, Resource<D>> implements ResourceType<D> {

    ResourceTypeImpl(Vertex v, Type type, AbstractGraknGraph graknGraph) {
        super(v, type, graknGraph);
    }

    ResourceTypeImpl(Vertex v, Type type, AbstractGraknGraph graknGraph, DataType<D> dataType, boolean isUnique) {
        super(v, type, graknGraph);

        setImmutableProperty(Schema.ConceptProperty.DATA_TYPE, dataType, getDataType(), DataType::getName);
        setImmutableProperty(Schema.ConceptProperty.IS_UNIQUE, isUnique, getProperty(Schema.ConceptProperty.IS_UNIQUE), Function.identity());
    }

    /**
     * @param regex The regular expression which instances of this resource must conform to.
     * @return The Resource Type itself.
     */
    @Override
    public ResourceType<D> setRegex(String regex) {
        if(!getDataType().equals(DataType.STRING)){
            throw new UnsupportedOperationException(ErrorMessage.REGEX_NOT_STRING.getMessage(toString()));
        }

        if(regex != null) {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher;
            for (Resource<D> resource : instances()) {
                String value = (String) resource.getValue();
                matcher = pattern.matcher(value);
                if(!matcher.matches()){
                    throw new InvalidConceptValueException(ErrorMessage.REGEX_INSTANCE_FAILURE.getMessage(regex, resource.toString()));
                }
            }
        }

        return setProperty(Schema.ConceptProperty.REGEX, regex);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Resource<D> putResource(D value) {
        Resource<D> resource = getResource(value);
        if(resource == null){
            resource = addInstance(Schema.BaseType.RESOURCE, (vertex, type) ->
                    getGraknGraph().getElementFactory().buildResource(vertex, type, value));
        }
        return resource;

    }

    @Override
    public <V> Resource<V> getResource(V value) {
        String index = ResourceImpl.generateResourceIndex(this, value.toString());
        Concept concept = getGraknGraph().getConcept(Schema.ConceptProperty.INDEX, index);
        if(concept != null){
            return concept.asResource();
        }
        return null;
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

    /**
     *
     * @return True if the resource type is unique and its instances are limited to one connection to an entity
     */
    @Override
    public Boolean isUnique() {
        return getProperty(Schema.ConceptProperty.IS_UNIQUE);
    }
}
