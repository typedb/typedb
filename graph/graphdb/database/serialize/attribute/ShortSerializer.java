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

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;
import grakn.core.graph.graphdb.database.serialize.OrderPreservingSerializer;

public class ShortSerializer implements OrderPreservingSerializer<Short> {

    private static final long serialVersionUID = 117423419862504186L;

    @Override
    public Short read(ScanBuffer buffer) {
        return (short) (buffer.getShort() + Short.MIN_VALUE);
    }

    @Override
    public void write(WriteBuffer out, Short object) {
        out.putShort((short)(object - Short.MIN_VALUE));
    }

    @Override
    public Short readByteOrder(ScanBuffer buffer) {
        return read(buffer);
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Short attribute) {
        write(buffer, attribute);
    }

    /*
    ====== These methods apply to all whole numbers with minor modifications ========
    ====== byte, short, int, long ======
     */

    @Override
    public Short convert(Object value) {
        if (value instanceof Number) {
            final double d = ((Number) value).doubleValue();
            Preconditions.checkArgument(!Double.isNaN(d) && Math.round(d) == d, "Not a valid short: " + value);
            final long l = ((Number) value).longValue();
            Preconditions.checkArgument(l >= Short.MIN_VALUE && l <= Short.MAX_VALUE,
                    "Value too large for short: " + value);
            return (short) l;
        } else if (value instanceof String) {
            return Short.parseShort((String) value);
        } else return null;
    }
}
