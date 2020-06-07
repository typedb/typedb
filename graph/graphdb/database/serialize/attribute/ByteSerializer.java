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

public class ByteSerializer implements OrderPreservingSerializer<Byte> {

    private static final long serialVersionUID = 117423419883604186L;

    @Override
    public Byte read(ScanBuffer buffer) {
        return (byte) (buffer.getByte() + Byte.MIN_VALUE);
    }

    @Override
    public void write(WriteBuffer out, Byte object) {
        out.putByte((byte)(object - Byte.MIN_VALUE));
    }

    @Override
    public Byte readByteOrder(ScanBuffer buffer) {
        return read(buffer);
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Byte attribute) {
        write(buffer,attribute);
    }

    /*
    ====== These methods apply to all whole numbers with minor modifications ========
    ====== boolean, byte, short, int, long ======
     */

    @Override
    public Byte convert(Object value) {
        if (value instanceof Number) {
            double d = ((Number)value).doubleValue();
            if (Double.isNaN(d) || Math.round(d)!=d) throw new IllegalArgumentException("Not a valid byte: " + value);
            long l = ((Number)value).longValue();
            if (l>=Byte.MIN_VALUE && l<=Byte.MAX_VALUE) return (byte) l;
            else throw new IllegalArgumentException("Value too large for byte: " + value);
        } else if (value instanceof String) {
            return Byte.parseByte((String)value);
        } else return null;
    }

}
