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
 */

package grakn.core.graph.core.schema;

import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.PropertyKey;

/**
 * Used to define new PropertyKeys.
 * An property key is defined by its name, Cardinality, its data type, and its signature - all of which
 * can be specified in this builder.
 */
public interface PropertyKeyMaker extends RelationTypeMaker {

    /**
     * Configures the Cardinality of this property key.
     *
     * @return this PropertyKeyMaker
     */
    PropertyKeyMaker cardinality(Cardinality cardinality);

    /**
     * Configures the data type for this property key.
     * <p>
     * Property instances for this key will only accept values that are instances of this class.
     * Every property key must have its data type configured. Setting the data type to Object.class allows
     * any type of value but comes at the expense of longer serialization because class information
     * is stored with the value.
     * <p>
     * It is strongly advised to pick an appropriate data type class so JanusGraph can enforce it throughout the database.
     *
     * @param clazz Data type to be configured.
     * @return this PropertyKeyMaker
     * see PropertyKey#dataType()
     */
    PropertyKeyMaker dataType(Class<?> clazz);

    @Override
    PropertyKeyMaker signature(PropertyKey... types);


    /**
     * Defines the PropertyKey specified by this PropertyKeyMaker and returns the resulting key.
     *
     * @return the created PropertyKey
     */
    @Override
    PropertyKey make();
}
