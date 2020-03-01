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

package grakn.core.graph.graphdb.database.serialize.attribute;

import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;
import grakn.core.graph.graphdb.database.serialize.OrderPreservingSerializer;
import grakn.core.graph.util.encoding.NumericUtils;


public class FloatSerializer implements OrderPreservingSerializer<Float> {

    private final IntegerSerializer ints = new IntegerSerializer();

    @Override
    public Float convert(Object value) {
        if (value instanceof Number) {
            final double d = ((Number)value).doubleValue();
            if (d < -Float.MAX_VALUE || d > Float.MAX_VALUE) throw new IllegalArgumentException("Value too large for float: " + value);
            return (float) d;
        } else if (value instanceof String) {
            return Float.parseFloat((String) value);
        } else return null;
    }

    @Override
    public Float read(ScanBuffer buffer) {
        return buffer.getFloat();
    }

    @Override
    public void write(WriteBuffer buffer, Float attribute) {
        buffer.putFloat(attribute);
    }

    @Override
    public Float readByteOrder(ScanBuffer buffer) {
        return NumericUtils.sortableIntToFloat(ints.readByteOrder(buffer));
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Float attribute) {
        ints.writeByteOrder(buffer, NumericUtils.floatToSortableInt(attribute));
    }
}
