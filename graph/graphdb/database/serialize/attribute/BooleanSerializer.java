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

public class BooleanSerializer implements OrderPreservingSerializer<Boolean> {

    @Override
    public Boolean read(ScanBuffer buffer) {
        return decode(buffer.getByte());
    }

    @Override
    public void write(WriteBuffer out, Boolean attribute) {
        out.putByte(encode(attribute));
    }


    @Override
    public Boolean readByteOrder(ScanBuffer buffer) {
        return read(buffer);
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Boolean attribute) {
        write(buffer,attribute);
    }

    @Override
    public Boolean convert(Object value) {
        if (value instanceof Number) {
            return decode(((Number)value).byteValue());
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String)value);
        } else return null;
    }

    public static boolean decode(byte b) {
        switch (b) {
            case 0: return false;
            case 1: return true;
            default: throw new IllegalArgumentException("Invalid boolean value: " + b);
        }
    }

    public static byte encode(boolean b) {
        return (byte)(b?1:0);
    }
}
