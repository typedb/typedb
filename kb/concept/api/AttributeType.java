/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.kb.concept.api;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.common.util.Collections.list;
import static grakn.common.util.Collections.set;

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

    Attribute<D> putAttributeInferred(D value);
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
     * see Attribute
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
    abstract class DataType<D> {
        public static final DataType<Boolean> BOOLEAN = new DataType<Boolean>(Boolean.class){
            @Override
            public Set<DataType<?>> comparableDataTypes() { return Collections.singleton(DataType.BOOLEAN); }
        };
        public static final DataType<LocalDateTime> DATE = new DataType<LocalDateTime>(LocalDateTime.class){
            @Override
            public Set<DataType<?>> comparableDataTypes() { return Collections.singleton(DataType.DATE); }
        };
        public static final DataType<Double> DOUBLE = new DataType<Double>(Double.class){
            @Override
            public Set<DataType<?>> comparableDataTypes() {
                return set(DataType.DOUBLE,
                        //DataType.FLOAT,
                        //DataType.INTEGER,
                        DataType.LONG);
            }
        };

        public static final DataType<Float> FLOAT = new DataType<Float>(Float.class){
            @Override
            public Set<DataType<?>> comparableDataTypes() { return new HashSet<>(); }
        };
        public static final DataType<Integer> INTEGER = new DataType<Integer>(Integer.class){
            @Override
            public Set<DataType<?>> comparableDataTypes() { return new HashSet<>(); }
        };
        public static final DataType<Long> LONG = new DataType<Long>(Long.class){
            @Override
            public Set<DataType<?>> comparableDataTypes() {
                return set(DataType.DOUBLE,
                        //DataType.FLOAT,
                        //DataType.INTEGER,
                        DataType.LONG);
            }
        };
        public static final DataType<String> STRING = new DataType<String>(String.class){
            @Override
            public Set<DataType<?>> comparableDataTypes() { return Collections.singleton(DataType.STRING); }
        };

        private static final List<DataType<?>> values = list(BOOLEAN, DATE, DOUBLE, FLOAT, INTEGER, LONG, STRING);

        private final Class<D> dataClass;

        private DataType(Class<D> dataClass) {
            this.dataClass = dataClass;
        }

        @CheckReturnValue
        public Class<D> dataClass() {
            return dataClass;
        }

        @CheckReturnValue
        public String name() {
            return dataClass.getName();
        }

        @Override
        public String toString() {
            return name();
        }

        @CheckReturnValue
        public static List<DataType<?>> values() {
            return values;
        }

        @CheckReturnValue
        public abstract Set<DataType<?>> comparableDataTypes();

        @SuppressWarnings("unchecked")
        @CheckReturnValue
        public static <D> DataType<D> of(Class<D> name) {
            for (DataType<?> dc : DataType.values()) {
                if (dc.dataClass.equals(name)) {
                    return (DataType<D>) dc;
                }
            }
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DataType<?> that = (DataType<?>) o;

            return (this.dataClass().equals(that.dataClass()));
        }

        @Override
        public int hashCode() {
            int h = 1;
            h *= 1000003;
            h ^=  dataClass.hashCode();
            return h;
        }
    }
}
