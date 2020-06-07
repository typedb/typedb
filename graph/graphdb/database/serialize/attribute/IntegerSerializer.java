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
import grakn.core.graph.graphdb.database.idhandling.VariableLong;
import grakn.core.graph.graphdb.database.serialize.OrderPreservingSerializer;

public class IntegerSerializer implements OrderPreservingSerializer<Integer> {

    private static final long serialVersionUID = 1174998819862504186L;

    @Override
    public Integer read(ScanBuffer buffer) {
        final long l = VariableLong.read(buffer);
        Preconditions.checkArgument(l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE,"Invalid serialization [%s]", l);
        return (int)l;
    }

    @Override
    public void write(WriteBuffer out, Integer attribute) {
        VariableLong.write(out,attribute);
    }

    @Override
    public Integer readByteOrder(ScanBuffer buffer) {
        return buffer.getInt() + Integer.MIN_VALUE;
    }

    @Override
    public void writeByteOrder(WriteBuffer out, Integer attribute) {
        out.putInt(attribute - Integer.MIN_VALUE);
    }

    /*
    ====== These methods apply to all whole numbers with minor modifications ========
    ====== byte, short, int, long ======
     */


    @Override
    public Integer convert(Object value) {
        if (value instanceof Number) {
            final double d = ((Number) value).doubleValue();
            Preconditions.checkArgument(!Double.isNaN(d) && Math.round(d) == d, "Not a valid integer: " + value);
            final long l = ((Number) value).longValue();
            Preconditions.checkArgument(l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE,
                "Value too large for integer: " + value);
            return (int) l;
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        } else return null;
    }
}
