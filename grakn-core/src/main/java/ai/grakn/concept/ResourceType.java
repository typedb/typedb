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


import ai.grakn.exception.InvalidConceptValueException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.function.Function;

/**
 * <p>
 *     An ontological element which models and categorises the various {@link Resource} in the graph.
 * </p>
 *
 * <p>
 *     This ontological element behaves similarly to {@link ai.grakn.concept.Type} when defining how it relates to other
 *     types. It has two additional functions to be aware of:
 *     1. It has a {@link DataType} constraining the data types of the values it's instances may take.
 *     2. Any of it's instances are unique to the type.
 *     For example if you have a ResourceType modelling month throughout the year there can only be one January.
 * </p>
 *
 * @author fppt
 *
 * @param <D> The data type of this resource type.
 *           Supported Types include: {@link String}, {@link Long}, {@link Double}, and {@link Boolean}
 */
public interface ResourceType<D> extends Type {
    //------------------------------------- Modifiers ----------------------------------
    /**
     * Sets the ResourceType to be abstract - which prevents it from having any instances.
     *
     * @param isAbstract  Specifies if the ResourceType is to be abstract (true) or not (false).
     *
     * @return The ResourceType itself.
     */
    ResourceType<D> setAbstract(Boolean isAbstract);

    /**
     * Sets the supertype of the ResourceType to be the ResourceType specified.
     *
     * @param type The super type of this ResourceType.
     * @return The ResourceType itself.
     */
    ResourceType<D> superType(ResourceType<D> type);

    /**
     * Adds another subtype to this type
     *
     * @param type The sub type of this resource type
     * @return The ResourceType itself
     */
    ResourceType<D> subType(ResourceType<D> type);

    /**
     * Sets the RoleType which instances of this ResourceType may play.
     *
     * @param roleType The Role Type which the instances of this ResourceType are allowed to play.
     * @return The ResourceType itself.
     */
    ResourceType<D> plays(RoleType roleType);

    /**
     * Removes the RoleType to prevent instances of this ResourceType from playing it.
     *
     * @param roleType The Role Type which the instances of this ResourceType should no longer be allowed to play.
     * @return The ResourceType itself.
     */
    ResourceType<D> deletePlays(RoleType roleType);

    /**
     * Set the regular expression that instances of the ResourceType must conform to.
     *
     * @param regex The regular expression that instances of this ResourceType must conform to.
     * @return The ResourceType itself.
     */
    ResourceType<D> setRegex(String regex);

    /**
     * Set the value for the Resource, unique to its type.
     *
     * @param value A value for the Resource which is unique to its type
     * @return new or existing Resource of this type with the provided value.
     */
    Resource<D> putResource(D value);

    /**
     * Classifies the type to a specific scope. This allows you to optionally categorise types.
     *
     * @param scope The category of this Type
     * @return The Type itself.
     */
    ResourceType<D> scope(Instance scope);

    /**
     * Delete the scope specified.
     *
     * @param scope The Instances that is currently scoping this Type.
     * @return The Type itself
     */
    ResourceType<D> deleteScope(Instance scope);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    ResourceType<D> key(ResourceType resourceType);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    ResourceType<D> resource(ResourceType resourceType);

    //------------------------------------- Accessors ---------------------------------
    /**
     * Returns the supertype of this ResourceType.
     *
     * @return The supertype of this ResourceType,
     */
    ResourceType<D> superType();

    /**
     * Get the Resource with the value provided, and its type, or return NULL
     * @see Resource
     *
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @param value A value which a Resource in the graph may be holding
     * @return The Resource with the provided value and type or null if no such Resource exists.
     */
    <V> Resource<V> getResource(V value);

    /**
     * Returns a collection of subtypes of this ResourceType.
     *
     * @return The subtypes of this ResourceType
     */
    Collection<ResourceType<D>> subTypes();

    /**
     * Returns a collection of all Resource Instances of this ResourceType.
     *
     * @return The resource instances of this ResourceType
     */
    Collection<Resource<D>> instances();

    /**
     * Get the data type to which instances of the ResourceType must conform.
     *
     * @return The data type to which instances of this resource must conform.
     */
    DataType<D> getDataType();

    /**
     * Retrieve the regular expression to which instances of this ResourceType must conform, or {@code null} if no
     * regular expression is set.
     *
     * By default, a {@link ResourceType} does not have a regular expression set.
     *
     * @return The regular expression to which instances of this ResourceType must conform.
     */
    String getRegex();

    /**
     *
     * @return a deep copy of this concept.
     */
    ResourceType<D> copy();

    /**
     * A class used to hold the supported data types of resources and any other concepts.
     * This is used tp constrain value data types to only those we explicitly support.
     * @param <D> The data type.
     */
    class DataType<D> {
        public static final DataType<String> STRING = new DataType<>(
                String.class.getName(),
                Schema.ConceptProperty.VALUE_STRING,
                (v) -> v,
                o -> defaultConverter(o, String.class, Object::toString));

        public static final DataType<Boolean> BOOLEAN = new DataType<>(
                Boolean.class.getName(),
                Schema.ConceptProperty.VALUE_BOOLEAN,
                (v) -> v,
                o -> defaultConverter(o, Boolean.class, (v) -> Boolean.parseBoolean(v.toString())));

        public static final DataType<Integer> INTEGER = new DataType<>(
                Integer.class.getName(),
                Schema.ConceptProperty.VALUE_INTEGER,
                (v) -> v,
                o -> defaultConverter(o, Integer.class, (v) -> Integer.parseInt(v.toString())));

        public static final DataType<Long> LONG = new DataType<>(
                Long.class.getName(),
                Schema.ConceptProperty.VALUE_LONG,
                (v) -> v,
                o -> defaultConverter(o, Long.class, (v) -> Long.parseLong(v.toString())));

        public static final DataType<Double> DOUBLE = new DataType<>(
                Double.class.getName(),
                Schema.ConceptProperty.VALUE_DOUBLE,
                (v) -> v,
                o -> defaultConverter(o, Double.class, (v) -> Double.parseDouble(v.toString())));

        public static final DataType<Float> FLOAT = new DataType<>(
                Float.class.getName(),
                Schema.ConceptProperty.VALUE_FLOAT,
                (v) -> v,
                o -> defaultConverter(o, Float.class, (v) -> Float.parseFloat(v.toString())));

        public static final DataType<LocalDateTime> DATE = new DataType<>(
                LocalDateTime.class.getName(),
                Schema.ConceptProperty.VALUE_DATE,
                (d) -> d.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                (o) -> {
                    if (o == null) return null;
                    if (!(o instanceof Long)) {
                        throw new InvalidConceptValueException(ErrorMessage.INVALID_DATATYPE.getMessage(o, Long.class.getName()));
                    }
                    return LocalDateTime.ofInstant(Instant.ofEpochMilli((long) o), ZoneId.systemDefault());
                });

        public static final ImmutableMap<String, DataType<?>> SUPPORTED_TYPES = ImmutableMap.<String, DataType<?>>builder()
                    .put(STRING.getName(), STRING)
                    .put(BOOLEAN.getName(), BOOLEAN)
                    .put(LONG.getName(), LONG)
                    .put(DOUBLE.getName(), DOUBLE)
                    .put(INTEGER.getName(), INTEGER)
                    .put(FLOAT.getName(), FLOAT)
                    .put(DATE.getName(), DATE)
                    .build();

        private final String dataType;
        private final Schema.ConceptProperty conceptProperty;
        private final Function<D, Object> persistenceValueSupplier;
        private final Function<Object, D> valueSupplier;


        private DataType(String dataType, Schema.ConceptProperty conceptProperty, Function<D, Object> savedValueProvider, Function<Object, D> valueSupplier){
            this.dataType = dataType;
            this.conceptProperty = conceptProperty;
            this.persistenceValueSupplier = savedValueProvider;
            this.valueSupplier = valueSupplier;
        }

        private static <X> X defaultConverter(Object o, Class clazz, Function<Object, X> converter){
            if(o == null){
                return null;
            } else if(clazz.isInstance(o)){
                //noinspection unchecked
                return (X) o;
            } else {
                return converter.apply(o);
            }
        }

        public String getName(){
            return dataType;
        }

        public Schema.ConceptProperty getConceptProperty(){
            return conceptProperty;
        }

        @Override
        public String toString(){
            return getName();
        }

        /**
         * Converts the provided value into the data type and format which it will be saved in.
         *
         * @param value The value to be converted
         * @return The String representation of the value
         */
        public Object getPersistenceValue(D value){
            return persistenceValueSupplier.apply(value);
        }

        /**
         * Converts the provided value into it's correct data type
         *
         * @param object The object to be converted into the value
         * @return The value of the string
         */
        public D getValue(Object object){
            return valueSupplier.apply(object);
        }
    }
}
