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

import grakn.core.graph.core.attribute.AttributeSerializer;
import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;

/**
 * Interface that extends AttributeSerializer to provide a serialization that is byte order preserving, i.e. the
 * order of the elements (as given by its Comparable implementation) corresponds to the natural order of the
 * serialized byte representation representation.
 *
 */
public interface OrderPreservingSerializer<V> extends AttributeSerializer<V> {


    /**
     * Reads an attribute from the given ReadBuffer assuming it was written in byte order.
     * <p>
     * It is expected that this read operation adjusts the position in the ReadBuffer to after the attribute value.
     *
     * @param buffer ReadBuffer to read attribute from
     * @return Read attribute
     */
    V readByteOrder(ScanBuffer buffer);

    /**
     * Writes the attribute value to the given WriteBuffer such that the byte order of the result is equal to the
     * natural order of the attribute.
     * <p>
     * It is expected that this write operation adjusts the position in the WriteBuffer to after the attribute value.
     *
     * @param buffer    WriteBuffer to write attribute to
     * @param attribute Attribute to write to WriteBuffer
     */
    void writeByteOrder(WriteBuffer buffer, V attribute);

}
