/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.concept;


import com.google.common.collect.ImmutableMap;
import grakn.core.graql.internal.Schema;
import grakn.core.server.exception.TransactionException;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * An ontological element which models and categorises the various {@link Attribute} in the graph.
 * This ontological element behaves similarly to {@link Type} when defining how it relates to other
 * types. It has two additional functions to be aware of:
 * 1. It has a {@link DataType} constraining the data types of the values it's instances may take.
 * 2. Any of it's instances are unique to the type.
 * For example if you have an {@link AttributeType} modelling month throughout the year there can only be one January.
 *
 * @param <D> The data type of this resource type.
 *            Supported Types include: {@link String}, {@link Long}, {@link Double}, and {@link Boolean}
 */
public interface AttributeType<D> extends Type {
    //------------------------------------- Modifiers ----------------------------------

    /**
     * Changes the {@link Label} of this {@link Concept} to a new one.
     *
     * @param label The new {@link Label}.
     * @return The {@link Concept} itself
     */
    AttributeType label(Label label);

    /**
     * Sets the {@link AttributeType} to be abstract - which prevents it from having any instances.
     *
     * @param isAbstract Specifies if the {@link AttributeType} is to be abstract (true) or not (false).
     * @return The {@link AttributeType} itself.
     */
    @Override
    AttributeType<D> isAbstract(Boolean isAbstract);

    /**
     * Sets the supertype of the {@link AttributeType} to be the AttributeType specified.
     *
     * @param type The super type of this {@link AttributeType}.
     * @return The {@link AttributeType} itself.
     */
    AttributeType<D> sup(AttributeType<D> type);

    /**
     * Sets the Role which instances of this {@link AttributeType} may play.
     *
     * @param role The Role Type which the instances of this {@link AttributeType} are allowed to play.
     * @return The {@link AttributeType} itself.
     */
    @Override
    AttributeType<D> plays(Role role);

    /**
     * Removes the ability of this {@link AttributeType} to play a specific {@link Role}
     *
     * @param role The {@link Role} which the {@link Thing}s of this {@link AttributeType} should no longer be allowed to play.
     * @return The {@link AttributeType} itself.
     */
    @Override
    AttributeType<D> unplay(Role role);

    /**
     * Removes the ability for {@link Thing}s of this {@link AttributeType} to have {@link Attribute}s of type {@link AttributeType}
     *
     * @param attributeType the {@link AttributeType} which this {@link AttributeType} can no longer have
     * @return The {@link AttributeType} itself.
     */
    @Override
    AttributeType<D> unhas(AttributeType attributeType);

    /**
     * Removes {@link AttributeType} as a key to this {@link AttributeType}
     *
     * @param attributeType the {@link AttributeType} which this {@link AttributeType} can no longer have as a key
     * @return The {@link AttributeType} itself.
     */
    @Override
    AttributeType<D> unkey(AttributeType attributeType);

    /**
     * Set the regular expression that instances of the {@link AttributeType} must conform to.
     *
     * @param regex The regular expression that instances of this {@link AttributeType} must conform to.
     * @return The {@link AttributeType} itself.
     */
    AttributeType<D> regex(String regex);

    /**
     * Set the value for the {@link Attribute}, unique to its type.
     *
     * @param value A value for the {@link Attribute} which is unique to its type
     * @return new or existing {@link Attribute} of this type with the provided value.
     */
    Attribute<D> create(D value);

    /**
     * Creates a {@link RelationType} which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param attributeType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    AttributeType<D> key(AttributeType attributeType);

    /**
     * Creates a {@link RelationType} which allows this type and a resource type to be linked.
     *
     * @param attributeType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    AttributeType<D> has(AttributeType attributeType);

    //------------------------------------- Accessors ---------------------------------

    /**
     * Returns the supertype of this {@link AttributeType}.
     *
     * @return The supertype of this {@link AttributeType},
     */
    @Override
    @Nullable
    AttributeType<D> sup();

    /**
     * Get the {@link Attribute} with the value provided, and its type, or return NULL
     *
     * @param value A value which an {@link Attribute} in the graph may be holding
     * @return The {@link Attribute} with the provided value and type or null if no such {@link Attribute} exists.
     * @see Attribute
     */
    @CheckReturnValue
    @Nullable
    Attribute<D> attribute(D value);

    /**
     * Returns a collection of super-types of this {@link AttributeType}.
     *
     * @return The super-types of this {@link AttributeType}
     */
    @Override
    Stream<AttributeType<D>> sups();

    /**
     * Returns a collection of subtypes of this {@link AttributeType}.
     *
     * @return The subtypes of this {@link AttributeType}
     */
    @Override
    Stream<AttributeType<D>> subs();

    /**
     * Returns a collection of all {@link Attribute} of this {@link AttributeType}.
     *
     * @return The resource instances of this {@link AttributeType}
     */
    @Override
    Stream<Attribute<D>> instances();

    /**
     * Get the data type to which instances of the {@link AttributeType} must conform.
     *
     * @return The data type to which instances of this {@link Attribute}  must conform.
     */
    @Nullable
    @CheckReturnValue
    DataType<D> dataType();

    /**
     * Retrieve the regular expression to which instances of this {@link AttributeType} must conform, or {@code null} if no
     * regular expression is set.
     * <p>
     * By default, an {@link AttributeType} does not have a regular expression set.
     *
     * @return The regular expression to which instances of this {@link AttributeType} must conform.
     */
    @CheckReturnValue
    @Nullable
    String regex();

    //------------------------------------- Other ---------------------------------
    @SuppressWarnings("unchecked")
    @Deprecated
    @CheckReturnValue
    @Override
    default AttributeType asAttributeType() {
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isAttributeType() {
        return true;
    }

    /**
     * A class used to hold the supported data types of resources and any other concepts.
     * This is used tp constrain value data types to only those we explicitly support.
     *
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
                        throw TransactionException.invalidAttributeValue(o, LONG);
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
        private final Function<D, Object> persistedValue;
        private final Function<Object, D> presentedValue;


        private DataType(String dataType, Schema.VertexProperty vertexProperty, Function<D, Object> persistedValue, Function<Object, D> presentedValue) {
            this.dataType = dataType;
            this.vertexProperty = vertexProperty;
            this.persistedValue = persistedValue;
            this.presentedValue = presentedValue;
        }

        private static <X> X defaultConverter(Object o, Class clazz, Function<Object, X> converter) {
            if (o == null) {
                return null;
            } else if (clazz.isInstance(o)) {
                //noinspection unchecked
                return (X) o;
            } else {
                return converter.apply(o);
            }
        }

        @CheckReturnValue
        public String getName() {
            return dataType;
        }

        @CheckReturnValue
        public Schema.VertexProperty getVertexProperty() {
            return vertexProperty;
        }

        @Override
        public String toString() {
            return getName();
        }

        /**
         * Converts the provided value into the data type and format which it will be saved in.
         *
         * @param value The value to be converted
         * @return The String representation of the value
         */
        @CheckReturnValue
        public Object getPersistedValue(D value) {
            return persistedValue.apply(value);
        }

        /**
         * Converts the provided value into it's correct data type
         *
         * @param object The object to be converted into the value
         * @return The value of the string
         */
        @CheckReturnValue
        public D getValue(Object object) {
            return presentedValue.apply(object);
        }
    }
}
