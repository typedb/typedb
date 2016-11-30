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

package ai.grakn.concept;


import ai.grakn.util.Schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A Resource Type which can hold different values.
 * @param <D> The data type of this resource type.
 */
public interface ResourceType<D> extends Type {
    //------------------------------------- Modifiers ----------------------------------
    /**
     *
     * @param isAbstract  Specifies if the Resource Type is abstract (true) or not (false).
     *                    If the Resource Type is abstract it is not allowed to have any instances.
     * @return The Resource Type itself.
     */
    ResourceType<D> setAbstract(Boolean isAbstract);

    /**
     *
     * @param type The super type of this Resource Type.
     * @return The Resource Type itself.
     */
    ResourceType<D> superType(ResourceType<D> type);

    /**
     *
     * @param roleType The Role Type which the instances of this Resource Type are allowed to play.
     * @return The Resource Type itself.
     */
    ResourceType<D> playsRole(RoleType roleType);

    /**
     *
     * @param roleType The Role Type which the instances of this Resource Type should no longer be allowed to play.
     * @return The Resource Type itself.
     */
    ResourceType<D> deletePlaysRole(RoleType roleType);

    /**
     * @param regex The regular expression which instances of this resource must conform to.
     * @return The Resource Type itself.
     */
    ResourceType<D> setRegex(String regex);

    /**
     * @param value A value for the Resource which is unique to it's type
     * @return new or existing Resource of this type with the provided value.
     */
    Resource<D> putResource(D value);

    //------------------------------------- Accessors ---------------------------------
    /**
     *
     * @return The super of this Resource Type
     */
    ResourceType<D> superType();

    /**
     *
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @param value A value which a Resource in the graph may be holding
     * @return The Resource with the provided value and type or null if no such Resource exists.
     */
    <V> Resource<V> getResource(V value);

    /**
     *
     * @return The sub types of this Resource Type
     */
    Collection<ResourceType<D>> subTypes();

    /**
     *
     * @return The resource instances of this Resource Type
     */
    Collection<Resource<D>> instances();

    /**
     * @return The data type which instances of this resource must conform to.
     */
    DataType<D> getDataType();

    /**
     * @return The regular expression which instances of this resource must conform to.
     */
    String getRegex();

    /**
     *
     * @return True if the resource type is unique and its instances are limited to one connection to an entity
     */
    Boolean isUnique();

    /**
     * A class used to hold the supported data types of resources and any other concepts.
     * This is used tp constrain value data types to only those we explicitly support.
     * @param <D> The data type.
     */
    class DataType<D> {
        public static final DataType<String> STRING = new DataType<>(String.class.getName(), Schema.ConceptProperty.VALUE_STRING);
        public static final DataType<Boolean> BOOLEAN = new DataType<>(Boolean.class.getName(), Schema.ConceptProperty.VALUE_BOOLEAN);
        public static final DataType<Long> LONG = new DataType<>(Long.class.getName(), Schema.ConceptProperty.VALUE_LONG);
        public static final DataType<Double> DOUBLE = new DataType<>(Double.class.getName(), Schema.ConceptProperty.VALUE_DOUBLE);
        public static final Map<String, DataType<?>> SUPPORTED_TYPES = new HashMap<>();

        static {
            SUPPORTED_TYPES.put(STRING.getName(), STRING);
            SUPPORTED_TYPES.put(BOOLEAN.getName(), BOOLEAN);
            SUPPORTED_TYPES.put(LONG.getName(), LONG);
            SUPPORTED_TYPES.put(DOUBLE.getName(), DOUBLE);
            SUPPORTED_TYPES.put(Integer.class.getName(), LONG);
        }

        private final String dataType;
        private final Schema.ConceptProperty conceptProperty;

        private DataType(String dataType, Schema.ConceptProperty conceptProperty){
            this.dataType = dataType;
            this.conceptProperty = conceptProperty;
        }

        public String getName(){
            return dataType;
        }

        public Schema.ConceptProperty getConceptProperty(){
            return conceptProperty;
        }
    }
}
