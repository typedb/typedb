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

package grakn.core.graph.core.attribute;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;

/**
 * Allows custom serializer definitions for attribute values.
 * <p>
 * For most data types (i.e. classes) used with properties, using the default serializer when registering the type with the
 * JanusGraph will be sufficient and efficient in practice. However, for certain data types, it can be more
 * efficient to provide custom serializers implementing this interface.
 * Such custom serializers are registered in the configuration file by specifying their path and loaded when
 * the database is initialized. Hence, the serializer must be on the classpath.
 * <br>
 * <p>
 * When a PropertyKey is defined using a data type specified via PropertyKeyMaker for which a custom serializer
 * is configured, then it will use this custom serializer for persistence operations.
 *
 * @param <V> Type of the attribute associated with the AttributeSerializer
 * see RelationTypeMaker
 * see <a href="https://docs.janusgraph.org/latest/serializer.html">
 *      "Datatype and Attribute Serializer Configuration" manual chapter</a>
 */
public interface AttributeSerializer<V> {

    /**
     * Reads an attribute from the given ReadBuffer.
     * <p>
     * It is expected that this read operation adjusts the position in the ReadBuffer to after the attribute value.
     *
     * @param buffer ReadBuffer to read attribute from
     * @return Read attribute
     */
    V read(ScanBuffer buffer);

    /**
     * Writes the attribute value to the given WriteBuffer.
     * <p>
     * It is expected that this write operation adjusts the position in the WriteBuffer to after the attribute value.
     *
     * @param buffer    WriteBuffer to write attribute to
     * @param attribute Attribute to write to WriteBuffer
     */
    void write(WriteBuffer buffer, V attribute);


    /**
     * Verifies the given (not-null) attribute value is valid.
     * Throws an IllegalArgumentException if the value is invalid,
     * otherwise simply returns.
     *
     * @param value to verify
     */
    default void verifyAttribute(V value) {
        Preconditions.checkNotNull(value,"Provided value cannot be null");
    }

    /**
     * Converts the given (not-null) value to the expected data type V.
     * The given object will NOT be of type V.
     * Throws an IllegalArgumentException if it cannot be converted.
     *
     * @param value to convert
     * @return converted to expected data type
     */
    default V convert(Object value) {
        try {
            return (V)value;
        } catch (ClassCastException e) {
            return null;
        }
    }

}
