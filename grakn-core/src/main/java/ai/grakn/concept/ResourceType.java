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


import ai.grakn.exception.GraphOperationException;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableMap;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * <p>
 *     An ontological element which models and categorises the various {@link Attribute} in the graph.
 * </p>
 *
 * <p>
 *     This ontological element behaves similarly to {@link ai.grakn.concept.Type} when defining how it relates to other
 *     types. It has two additional functions to be aware of:
 *     1. It has a {@link DataType} constraining the data types of the values it's instances may take.
 *     2. Any of it's instances are unique to the type.
 *     For example if you have a {@link ResourceType} modelling month throughout the year there can only be one January.
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
     * Changes the {@link Label} of this {@link Concept} to a new one.
     * @param label The new {@link Label}.
     * @return The {@link Concept} itself
     */
    ResourceType setLabel(Label label);

    /**
     * Sets the {@link ResourceType} to be abstract - which prevents it from having any instances.
     *
     * @param isAbstract  Specifies if the {@link ResourceType} is to be abstract (true) or not (false).
     *
     * @return The {@link ResourceType} itself.
     */
    @Override
    ResourceType<D> setAbstract(Boolean isAbstract);

    /**
     * Sets the supertype of the ResourceType to be the ResourceType specified.
     *
     * @param type The super type of this ResourceType.
     * @return The ResourceType itself.
     */
    ResourceType<D> sup(ResourceType<D> type);

    /**
     * Adds another subtype to this type
     *
     * @param type The sub type of this {@link ResourceType}
     * @return The {@link ResourceType} itself
     */
    ResourceType<D> sub(ResourceType<D> type);

    /**
     * Sets the Role which instances of this {@link ResourceType} may play.
     *
     * @param role The Role Type which the instances of this {@link ResourceType} are allowed to play.
     * @return The {@link ResourceType} itself.
     */
    @Override
    ResourceType<D> plays(Role role);

    /**
     * Removes the Role to prevent instances of this {@link ResourceType} from playing it.
     *
     * @param role The Role Type which the instances of this {@link ResourceType} should no longer be allowed to play.
     * @return The {@link ResourceType} itself.
     */
    @Override
    ResourceType<D> deletePlays(Role role);

    /**
     * Set the regular expression that instances of the {@link ResourceType} must conform to.
     *
     * @param regex The regular expression that instances of this {@link ResourceType} must conform to.
     * @return The {@link ResourceType} itself.
     */
    ResourceType<D> setRegex(String regex);

    /**
     * Set the value for the {@link Attribute}, unique to its type.
     *
     * @param value A value for the {@link Attribute} which is unique to its type
     * @return new or existing {@link Attribute} of this type with the provided value.
     */
    Attribute<D> putResource(D value);

    /**
     * Classifies the type to a specific scope. This allows you to optionally categorise types.
     *
     * @param scope The category of this Type
     * @return The Type itself.
     */
    @Override
    ResourceType<D> scope(Thing scope);

    /**
     * Delete the scope specified.
     *
     * @param scope The Instances that is currently scoping this Type.
     * @return The Type itself
     */
    @Override
    ResourceType<D> deleteScope(Thing scope);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    ResourceType<D> key(ResourceType resourceType);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    ResourceType<D> resource(ResourceType resourceType);

    //------------------------------------- Accessors ---------------------------------
    /**
     * Returns the supertype of this {@link ResourceType}.
     *
     * @return The supertype of this {@link ResourceType},
     */
    @Override
    @Nonnull
    ResourceType<D> sup();

    /**
     * Get the {@link Attribute} with the value provided, and its type, or return NULL
     * @see Attribute
     *
     * @param value A value which a {@link Attribute} in the graph may be holding
     * @return The {@link Attribute} with the provided value and type or null if no such {@link Attribute} exists.
     */
    @CheckReturnValue
    @Nullable
    Attribute<D> getResource(D value);

    /**
     * Returns a collection of subtypes of this {@link ResourceType}.
     *
     * @return The subtypes of this {@link ResourceType}
     */
    @Override
    Stream<ResourceType<D>> subs();

    /**
     * Returns a collection of all {@link Attribute} of this {@link ResourceType}.
     *
     * @return The resource instances of this {@link ResourceType}
     */
    @Override
    Stream<Attribute<D>> instances();

    /**
     * Get the data type to which instances of the {@link ResourceType} must conform.
     *
     * @return The data type to which instances of this {@link Attribute}  must conform.
     */
    @CheckReturnValue
    DataType<D> getDataType();

    /**
     * Retrieve the regular expression to which instances of this {@link ResourceType} must conform, or {@code null} if no
     * regular expression is set.
     *
     * By default, a {@link ResourceType} does not have a regular expression set.
     *
     * @return The regular expression to which instances of this {@link ResourceType} must conform.
     */
    @CheckReturnValue
    @Nullable
    String getRegex();

    //------------------------------------- Other ---------------------------------
    @SuppressWarnings("unchecked")
    @Deprecated
    @CheckReturnValue
    @Override
    default ResourceType asResourceType(){
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isResourceType(){
        return true;
    }

    /**
     * A class used to hold the supported data types of resources and any other concepts.
     * This is used tp constrain value data types to only those we explicitly support.
     * @param <D> The data type.
     */
    class DataType<D> {
        public static final DataType<String> STRING = new DataType<>(
                String.class.getName(),
                Schema.VertexProperty.VALUE_STRING,
                (v) -> v,
                o -> defaultConverter(o, String.class, Object::toString));

        public static final DataType<Boolean> BOOLEAN = new DataType<>(
                Boolean.class.getName(),
                Schema.VertexProperty.VALUE_BOOLEAN,
                (v) -> v,
                o -> defaultConverter(o, Boolean.class, (v) -> Boolean.parseBoolean(v.toString())));

        public static final DataType<Integer> INTEGER = new DataType<>(
                Integer.class.getName(),
                Schema.VertexProperty.VALUE_INTEGER,
                (v) -> v,
                o -> defaultConverter(o, Integer.class, (v) -> Integer.parseInt(v.toString())));

        public static final DataType<Long> LONG = new DataType<>(
                Long.class.getName(),
                Schema.VertexProperty.VALUE_LONG,
                (v) -> v,
                o -> defaultConverter(o, Long.class, (v) -> Long.parseLong(v.toString())));

        public static final DataType<Double> DOUBLE = new DataType<>(
                Double.class.getName(),
                Schema.VertexProperty.VALUE_DOUBLE,
                (v) -> v,
                o -> defaultConverter(o, Double.class, (v) -> Double.parseDouble(v.toString())));

        public static final DataType<Float> FLOAT = new DataType<>(
                Float.class.getName(),
                Schema.VertexProperty.VALUE_FLOAT,
                (v) -> v,
                o -> defaultConverter(o, Float.class, (v) -> Float.parseFloat(v.toString())));

        public static final DataType<LocalDateTime> DATE = new DataType<>(
                LocalDateTime.class.getName(),
                Schema.VertexProperty.VALUE_DATE,
                (d) -> d.atZone(ZoneId.of("Z")).toInstant().toEpochMilli(),
                (o) -> {
                    if (o == null) return null;
                    if (!(o instanceof Long)) {
                        throw GraphOperationException.invalidResourceValue(o, LONG);
                    }
                    return LocalDateTime.ofInstant(Instant.ofEpochMilli((long) o), ZoneId.of("Z"));
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
        private final Schema.VertexProperty vertexProperty;
        private final Function<D, Object> persistenceValueSupplier;
        private final Function<Object, D> valueSupplier;


        private DataType(String dataType, Schema.VertexProperty vertexProperty, Function<D, Object> savedValueProvider, Function<Object, D> valueSupplier){
            this.dataType = dataType;
            this.vertexProperty = vertexProperty;
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

        @CheckReturnValue
        public String getName(){
            return dataType;
        }

        @CheckReturnValue
        public Schema.VertexProperty getVertexProperty(){
            return vertexProperty;
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
        @CheckReturnValue
        public Object getPersistenceValue(D value){
            return persistenceValueSupplier.apply(value);
        }

        /**
         * Converts the provided value into it's correct data type
         *
         * @param object The object to be converted into the value
         * @return The value of the string
         */
        @CheckReturnValue
        public D getValue(Object object){
            return valueSupplier.apply(object);
        }
    }
}
