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

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * An ontological element which models and categorises the various Attribute in the graph.
 * This ontological element behaves similarly to Type when defining how it relates to other
 * types. It has two additional functions to be aware of:
 * 1. It has a DataType constraining the data types of the values it's instances may take.
 * 2. Any of it's instances are unique to the type.
 * For example if you have an AttributeType modelling month throughout the year there can only be one January.
 *
 * @param <D> The data type of this resource type.
 *            Supported Types include: String, Long, Double, and Boolean
 */
public interface AttributeType<D> extends Type {
    //------------------------------------- Modifiers ----------------------------------

    /**
     * Changes the Label of this Concept to a new one.
     *
     * @param label The new Label.
     * @return The Concept itself
     */
    AttributeType label(Label label);

    /**
     * Sets the AttributeType to be abstract - which prevents it from having any instances.
     *
     * @param isAbstract Specifies if the AttributeType is to be abstract (true) or not (false).
     * @return The AttributeType itself.
     */
    @Override
    AttributeType<D> isAbstract(Boolean isAbstract);

    /**
     * Sets the supertype of the AttributeType to be the AttributeType specified.
     *
     * @param type The super type of this AttributeType.
     * @return The AttributeType itself.
     */
    AttributeType<D> sup(AttributeType<D> type);

    /**
     * Sets the Role which instances of this AttributeType may play.
     *
     * @param role The Role Type which the instances of this AttributeType are allowed to play.
     * @return The AttributeType itself.
     */
    @Override
    AttributeType<D> plays(Role role);

    /**
     * Removes the ability of this AttributeType to play a specific Role
     *
     * @param role The Role which the Things of this AttributeType should no longer be allowed to play.
     * @return The AttributeType itself.
     */
    @Override
    AttributeType<D> unplay(Role role);

    /**
     * Removes the ability for Things of this AttributeType to have Attributes of type AttributeType
     *
     * @param attributeType the AttributeType which this AttributeType can no longer have
     * @return The AttributeType itself.
     */
    @Override
    AttributeType<D> unhas(AttributeType attributeType);

    /**
     * Removes AttributeType as a key to this AttributeType
     *
     * @param attributeType the AttributeType which this AttributeType can no longer have as a key
     * @return The AttributeType itself.
     */
    @Override
    AttributeType<D> unkey(AttributeType attributeType);

    /**
     * Set the regular expression that instances of the AttributeType must conform to.
     *
     * @param regex The regular expression that instances of this AttributeType must conform to.
     * @return The AttributeType itself.
     */
    AttributeType<D> regex(String regex);

    /**
     * Set the value for the Attribute, unique to its type.
     *
     * @param value A value for the Attribute which is unique to its type
     * @return new or existing Attribute of this type with the provided value.
     */
    Attribute<D> create(D value);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param attributeType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    AttributeType<D> key(AttributeType attributeType);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked.
     *
     * @param attributeType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    AttributeType<D> has(AttributeType attributeType);

    //------------------------------------- Accessors ---------------------------------

    /**
     * Returns the supertype of this AttributeType.
     *
     * @return The supertype of this AttributeType,
     */
    @Override
    @Nullable
    AttributeType<D> sup();

    /**
     * Get the Attribute with the value provided, and its type, or return NULL
     *
     * @param value A value which an Attribute in the graph may be holding
     * @return The Attribute with the provided value and type or null if no such Attribute exists.
     * @see Attribute
     */
    @CheckReturnValue
    @Nullable
    Attribute<D> attribute(D value);

    /**
     * Returns a collection of super-types of this AttributeType.
     *
     * @return The super-types of this AttributeType
     */
    @Override
    Stream<AttributeType<D>> sups();

    /**
     * Returns a collection of subtypes of this AttributeType.
     *
     * @return The subtypes of this AttributeType
     */
    @Override
    Stream<AttributeType<D>> subs();

    /**
     * Returns a collection of all Attribute of this AttributeType.
     *
     * @return The resource instances of this AttributeType
     */
    @Override
    Stream<Attribute<D>> instances();

    /**
     * Get the data type to which instances of the AttributeType must conform.
     *
     * @return The data type to which instances of this Attribute  must conform.
     */
    @Nullable
    @CheckReturnValue
    DataType<D> dataType();

    /**
     * Retrieve the regular expression to which instances of this AttributeType must conform, or {@code null} if no
     * regular expression is set.
     * By default, an AttributeType does not have a regular expression set.
     *
     * @return The regular expression to which instances of this AttributeType must conform.
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
                String.class,
                (v) -> v
        );

        public static final DataType<Boolean> BOOLEAN = new DataType<>(
                Boolean.class,
                (v) -> v
        );

        public static final DataType<Integer> INTEGER = new DataType<>(
                Integer.class,
                (v) -> v
        );

        public static final DataType<Long> LONG = new DataType<>(
                Long.class,
                (v) -> v
        );

        public static final DataType<Double> DOUBLE = new DataType<>(
                Double.class,
                (v) -> v
        );

        public static final DataType<Float> FLOAT = new DataType<>(
                Float.class,
                (v) -> v
        );

        public static final DataType<LocalDateTime> DATE = new DataType<>(
                LocalDateTime.class,
                (d) -> d.atZone(ZoneId.of("Z")).toInstant().toEpochMilli()
        );

        public static final ImmutableMap<Class, DataType<?>> SUPPORTED_TYPES = ImmutableMap.<Class, DataType<?>>builder()
                .put(STRING.getValueClass(), STRING)
                .put(BOOLEAN.getValueClass(), BOOLEAN)
                .put(LONG.getValueClass(), LONG)
                .put(DOUBLE.getValueClass(), DOUBLE)
                .put(INTEGER.getValueClass(), INTEGER)
                .put(FLOAT.getValueClass(), FLOAT)
                .put(DATE.getValueClass(), DATE)
                .build();

        private final Class<D> dataType;
        private final Function<D, Object> persistedValue;


        private DataType(Class<D> dataType, Function<D, Object> persistedValue) {
            this.dataType = dataType;
            this.persistedValue = persistedValue;
        }

        public Class<D> getValueClass() {
            return dataType;
        }

        @CheckReturnValue
        public String getName() {
            return dataType.getName();
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
    }
}
