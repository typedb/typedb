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

import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.exception.InvalidConceptValueException;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Resource Type which can hold different values.
 * @param <D> The data tyoe of this resource type.
 */
class ResourceTypeImpl<D> extends TypeImpl<ResourceType<D>, Resource<D>> implements ResourceType<D> {

    ResourceTypeImpl(Vertex v, AbstractMindmapsGraph mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    ResourceTypeImpl(Vertex v, AbstractMindmapsGraph mindmapsGraph, DataType<D> type) {
        super(v, mindmapsGraph);
        setDataType(type);
    }

    /**
     *
     * @param type The data type of the resource
     */
    private void setDataType(DataType<D> type) {
        setProperty(Schema.ConceptProperty.DATA_TYPE, type.getName());
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

    /**
     * @return The data type which instances of this resource must conform to.
     */
    //This unsafe cast is suppressed because at this stage we do not know what the type is when reading from the rootGraph.
    @SuppressWarnings("unchecked")
    @Override
    public DataType<D> getDataType() {
        Object object = getProperty(Schema.ConceptProperty.DATA_TYPE);
        return (DataType<D>) DataType.SUPPORTED_TYPES.get(String.valueOf(object));
    }

    /**
     * @return The regular expression which instances of this resource must conform to.
     */
    @Override
    public String getRegex() {
        Object object = getProperty(Schema.ConceptProperty.REGEX);
        if(object == null)
            return null;
        return (String) object;
    }
}
