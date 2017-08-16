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


import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * <p>
 *     Represent a literal {@link Resource} in the graph.
 * </p>
 *
 * <p>
 *     Acts as an {@link Thing} when relating to other instances except it has the added functionality of:
 *     1. It is unique to its {@link ResourceType} based on it's value.
 *     2. It has a {@link ai.grakn.concept.ResourceType.DataType} associated with it which constrains the allowed values.
 * </p>
 *
 * @author fppt
 *
 * @param <D> The data type of this resource type.
 *           Supported Types include: {@link String}, {@link Long}, {@link Double}, and {@link Boolean}
 */
public interface Resource<D> extends Thing {
    //------------------------------------- Accessors ----------------------------------
    /**
     * Retrieves the value of the {@link Resource}.
     *
     * @return The value itself
     */
    @CheckReturnValue
    D getValue();

    /**
     * Retrieves the type of the {@link Resource}, that is, the {@link ResourceType} of which this resource is an Thing.
     *
     * @return The {@link ResourceType of which this resource is an Thing.
     */
    @Override
    ResourceType<D> type();

    /**
     * Retrieves the data type of this {@link Resource}'s {@link ResourceType}.
     *
     * @return The data type of this {@link Resource}'s type.
     */
    @CheckReturnValue
    ResourceType.DataType<D> dataType();

    /**
     * Retrieves the set of all Instances that possess this {@link Resource}.
     *
     * @return The list of all Instances that possess this {@link Resource}.
     */
    @CheckReturnValue
    Stream<Thing> ownerInstances();

    /**
     * If the {@link Resource} is unique, this method retrieves the Thing that possesses it.
     *
     * @return The Thing which is connected to a unique {@link Resource}.
     */
    @CheckReturnValue
    @Nullable
    Thing owner();

    /**
     * Creates a relation from this instance to the provided {@link Resource}.
     *
     * @param resource The {@link Resource} to which a relationship is created
     * @return The instance itself
     */
    @Override
    Resource resource(Resource resource);

    //------------------------------------- Other ---------------------------------
    @SuppressWarnings("unchecked")
    @Deprecated
    @CheckReturnValue
    @Override
    default Resource asResource(){
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isResource(){
        return true;
    }
}
