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


public class DoubleSerializer implements OrderPreservingSerializer<Double> {

    private final LongSerializer longs = new LongSerializer();

    @Override
    public Double convert(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            return Double.parseDouble((String) value);
        } else return null;
    }

    @Override
    public Double read(ScanBuffer buffer) {
        return buffer.getDouble();
    }

    @Override
    public void write(WriteBuffer buffer, Double attribute) {
        buffer.putDouble(attribute);
    }

    @Override
    public Double readByteOrder(ScanBuffer buffer) {
        return NumericUtils.sortableLongToDouble(longs.readByteOrder(buffer));
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Double attribute) {
        longs.writeByteOrder(buffer, NumericUtils.doubleToSortableLong(attribute));
    }
}
