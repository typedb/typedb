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

import java.util.HashMap;
import java.util.Map;

/**
 * A class used to hold the supported data types of resources and any other concepts
 * @param <D>
 */
public class Data<D> {
    public static final Data<String> STRING = new Data<>(String.class.getName(), DataType.ConceptProperty.VALUE_STRING);
    public static final Data<Boolean> BOOLEAN = new Data<>(Boolean.class.getName(), DataType.ConceptProperty.VALUE_BOOLEAN);
    public static final Data<Long> LONG = new Data<>(Long.class.getName(), DataType.ConceptProperty.VALUE_LONG);
    public static final Data<Double> DOUBLE = new Data<>(Double.class.getName(), DataType.ConceptProperty.VALUE_DOUBLE);
    public static final Map<String, Data<?>> SUPPORTED_TYPES = new HashMap<>();

    static {
        SUPPORTED_TYPES.put(STRING.getName(), STRING);
        SUPPORTED_TYPES.put(BOOLEAN.getName(), BOOLEAN);
        SUPPORTED_TYPES.put(LONG.getName(), LONG);
        SUPPORTED_TYPES.put(DOUBLE.getName(), DOUBLE);
        SUPPORTED_TYPES.put(Integer.class.getName(), LONG);
    }

    private final String dataType;
    private final DataType.ConceptProperty conceptProperty;

    private Data(String dataType, DataType.ConceptProperty conceptProperty){
        this.dataType = dataType;
        this.conceptProperty = conceptProperty;
    }

    public String getName(){
        return dataType;
    }

    public DataType.ConceptProperty getConceptProperty(){
        return conceptProperty;
    }
}
