/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.concept;


import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * <p>
 *     Represent a literal {@link Attribute} in the graph.
 * </p>
 *
 * <p>
 *     Acts as an {@link Thing} when relating to other instances except it has the added functionality of:
 *     1. It is unique to its {@link AttributeType} based on it's value.
 *     2. It has an {@link AttributeType.DataType} associated with it which constrains the allowed values.
 * </p>
 *
 * @author fppt
 *
 * @param <D> The data type of this resource type.
 *           Supported Types include: {@link String}, {@link Long}, {@link Double}, and {@link Boolean}
 */
public interface Attribute<D> extends Thing {
    //------------------------------------- Accessors ----------------------------------
    /**
     * Retrieves the value of the {@link Attribute}.
     *
     * @return The value itself
     */
    @CheckReturnValue
    D value();

    /**
     * Retrieves the type of the {@link Attribute}, that is, the {@link AttributeType} of which this resource is an Thing.
     *
     * @return The {@link AttributeType of which this resource is an Thing.
     */
    @Override
    AttributeType<D> type();

    /**
     * Retrieves the data type of this {@link Attribute}'s {@link AttributeType}.
     *
     * @return The data type of this {@link Attribute}'s type.
     */
    @CheckReturnValue
    AttributeType.DataType<D> dataType();

    /**
     * Retrieves the set of all Instances that possess this {@link Attribute}.
     *
     * @return The list of all Instances that possess this {@link Attribute}.
     */
    @CheckReturnValue
    Stream<Thing> owners();

    /**
     * If the {@link Attribute} is unique, this method retrieves the Thing that possesses it.
     *
     * @return The Thing which is connected to a unique {@link Attribute}.
     */
    @CheckReturnValue
    @Nullable
    default Thing owner() {
        Iterator<Thing> owners = owners().iterator();
        if(owners.hasNext()) {
            return owners.next();
        } else {
            return null;
        }
    }

    /**
     * Creates a relation from this instance to the provided {@link Attribute}.
     *
     * @param attribute The {@link Attribute} to which a relationship is created
     * @return The instance itself
     */
    @Override
    Attribute has(Attribute attribute);

    /**
     * Removes the provided {@link Attribute} from this {@link Attribute}
     * @param attribute the {@link Attribute} to be removed
     * @return The {@link Attribute} itself
     */
    @Override
    Attribute unhas(Attribute attribute);

    //------------------------------------- Other ---------------------------------
    @SuppressWarnings("unchecked")
    @Deprecated
    @CheckReturnValue
    @Override
    default Attribute asAttribute(){
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isAttribute(){
        return true;
    }
}
