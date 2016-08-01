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

package io.mindmaps.core.implementation;

import io.mindmaps.core.exceptions.ConceptException;
import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.exceptions.InvalidConceptValueException;
import io.mindmaps.core.model.Resource;
import io.mindmaps.core.model.ResourceType;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ResourceTypeImpl<D> extends TypeImpl<ResourceType<D>, Resource<D>> implements ResourceType<D> {

    ResourceTypeImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    ResourceTypeImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph, Data<D> type) {
        super(v, mindmapsGraph);
        setDataType(type);
    }

    private void setDataType(Data<D> type) {
        setProperty(DataType.ConceptProperty.DATA_TYPE, type.getName());
    }

    @Override
    public ResourceType<D> setRegex(String regex) {
        if(!getDataType().equals(Data.STRING)){
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

        return setProperty(DataType.ConceptProperty.REGEX, regex);
    }

    @Override
    public ResourceType<D> setUnique(boolean isUnique) {
        if(isUnique){
            if(instancesUnique()){
                return setProperty(DataType.ConceptProperty.IS_UNIQUE, true);
            } else {
                throw new ConceptException(ErrorMessage.RESOURCE_TYPE_CANNOT_BE_UNIQUE.getMessage(this));
            }
        } else {
            resetInstanceIndices();
            return setProperty(DataType.ConceptProperty.IS_UNIQUE, false);
        }
    }

    private void resetInstanceIndices(){
        instances().forEach(resource -> {
            ((ResourceImpl<D>) resource).setUniqueProperty(DataType.ConceptPropertyUnique.INDEX, null);
        });
    }

    private boolean instancesUnique(){
        Set<D> values = new HashSet<>();
        for(Resource<D> instance : instances()){
            D foundValue = instance.getValue();
            if(values.contains(foundValue)){
                return false;
            } else {
                values.add(foundValue);
                ResourceImpl<D> resource = ((ResourceImpl<D>) instance);
                String newIndex = resource.generateResourceIndex(foundValue);
                resource.setUniqueProperty(DataType.ConceptPropertyUnique.INDEX, newIndex);
            }
        }
        return true;
    }

    //This unsafe cast is suppressed because at this stage we do not know what the type is when reading from the rootGraph.
    @SuppressWarnings("unchecked")
    @Override
    public Data<D> getDataType() {
        Object object = getProperty(DataType.ConceptProperty.DATA_TYPE);
        return (Data<D>) Data.SUPPORTED_TYPES.get(String.valueOf(object));
    }

    @Override
    public String getRegex() {
        Object object = getProperty(DataType.ConceptProperty.REGEX);
        if(object == null)
            return null;
        return (String) object;
    }

    @Override
    public boolean isUnique() {
        Object object = getProperty(DataType.ConceptProperty.IS_UNIQUE);
        return object != null && Boolean.parseBoolean(object.toString());
    }
}
