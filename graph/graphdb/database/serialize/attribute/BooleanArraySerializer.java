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

import grakn.core.graph.core.attribute.AttributeSerializer;
import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;

import java.lang.reflect.Array;

public class BooleanArraySerializer extends ArraySerializer implements AttributeSerializer<boolean[]> {

    @Override
    public boolean[] convert(Object value) {
        return convertInternal(value, boolean.class, Boolean.class);
    }

    @Override
    protected Object getArray(int length) {
        return new boolean[length];
    }

    @Override
    protected void setArray(Object array, int pos, Object value) {
        Array.setBoolean(array, pos, ((Boolean) value));
    }

    //############### Serialization ###################

    @Override
    public boolean[] read(ScanBuffer buffer) {
        int length = getLength(buffer);
        if (length<0) return null;
        boolean[] result = new boolean[length];
        int b = 0;
        for (int i = 0; i < length; i++) {
            int offset = i%8;
            if (offset==0) {
                b= 0xFF & buffer.getByte();
            }
            result[i]= BooleanSerializer.decode((byte)((b>>>(7-offset))&1));
        }
        return result;
    }

    @Override
    public void write(WriteBuffer buffer, boolean[] attribute) {
        writeLength(buffer,attribute);
        if (attribute==null) return;
        byte b = 0;
        int i = 0;
        for (; i < attribute.length; i++) {
            b = (byte)( ((int)b<<1) | BooleanSerializer.encode(attribute[i]));
            if ((i+1)%8 == 0) {
                buffer.putByte(b);
                b=0;
            }
        }
        if (i%8!=0) buffer.putByte((byte)(b<<(8-(i%8))));
    }
}
