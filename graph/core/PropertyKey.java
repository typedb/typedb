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


package grakn.core.graph.core;

/**
 * PropertyKey is an extension of RelationType for properties. Each property in JanusGraph has a key.
 * <p>
 * A property key defines the following characteristics of a property:
 * <ul>
 * <li><strong>Data Type:</strong> The data type of the value for a given property of this key</li>
 * <li><strong>Cardinality:</strong> The cardinality of the set of properties that may be associated with a single
 * vertex through a particular key.
 * </li>
 * </ul>
 *
 * see RelationType
 */
public interface PropertyKey extends RelationType {

    /**
     * Returns the data type for this property key.
     * The values of all properties of this type must be an instance of this data type.
     *
     * @return Data type for this property key.
     */
    Class<?> dataType();

    /**
     * The Cardinality of this property key.
     */
    Cardinality cardinality();

}
