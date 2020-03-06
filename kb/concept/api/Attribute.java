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
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Represent a literal Attribute in the graph.
 * Acts as an Thing when relating to other instances except it has the added functionality of:
 * 1. It is unique to its AttributeType based on it's value.
 * 2. It has an AttributeType.DataType associated with it which constrains the allowed values.
 *
 * @param <D> The data type of this resource type.
 *            Supported Types include: String, Long, Double, and Boolean
 */
public interface Attribute<D> extends Thing {
    //------------------------------------- Accessors ----------------------------------

    /**
     * Retrieves the value of the Attribute.
     *
     * @return The value itself
     */
    @CheckReturnValue
    D value();

    /**
     * Retrieves the type of the Attribute, that is, the AttributeType of which this resource is an Thing.
     *
     * @return The AttributeType of which this resource is an Thing.
     */
    @Override
    AttributeType<D> type();

    /**
     * Retrieves the data type of this Attribute's AttributeType.
     *
     * @return The data type of this Attribute's type.
     */
    @CheckReturnValue
    AttributeType.DataType<D> dataType();

    /**
     * Retrieves the set of all Instances that possess this Attribute.
     *
     * @return The list of all Instances that possess this Attribute.
     */
    @CheckReturnValue
    Stream<Thing> owners();

    /**
     * If the Attribute is unique, this method retrieves the Thing that possesses it.
     *
     * @return The Thing which is connected to a unique Attribute.
     */
    @CheckReturnValue
    @Nullable
    default Thing owner() {
        Iterator<Thing> owners = owners().iterator();
        if (owners.hasNext()) {
            return owners.next();
        } else {
            return null;
        }
    }

    /**
     * Creates a relation from this instance to the provided Attribute.
     *
     * @param attribute The Attribute to which a relation is created
     * @return The instance itself
     */
    @Override
    Attribute has(Attribute attribute);

    /**
     * Removes the provided Attribute from this Attribute
     *
     * @param attribute the Attribute to be removed
     * @return The Attribute itself
     */
    @Override
    Attribute unhas(Attribute attribute);

    //------------------------------------- Other ---------------------------------
    @SuppressWarnings("unchecked")
    @Deprecated
    @CheckReturnValue
    @Override
    default Attribute asAttribute() {
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isAttribute() {
        return true;
    }
}
