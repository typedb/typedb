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

package grakn.core.graph.graphdb.database.serialize;

public interface AttributeHandler {

    boolean validDataType(Class datatype);

    <V> void verifyAttribute(Class<V> datatype, Object value);

    /**
     * Converts the given (not-null) value to the this datatype V.
     * The given object will NOT be of type V.
     * Throws an IllegalArgumentException if it cannot be converted.
     *
     * @param value to convert
     * @return converted to expected datatype
     */
    <V> V convert(Class<V> datatype, Object value);

    boolean isOrderPreservingDatatype(Class<?> datatype);

}
