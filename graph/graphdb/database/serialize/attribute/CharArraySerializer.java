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

public class CharArraySerializer extends ArraySerializer implements AttributeSerializer<char[]> {


    @Override
    public char[] convert(Object value) {
        return convertInternal(value, char.class, Character.class);
    }

    @Override
    protected Object getArray(int length) {
        return new char[length];
    }

    @Override
    protected void setArray(Object array, int pos, Object value) {
        Array.setChar(array, pos, ((Character) value));
    }

    //############### Serialization ###################

    @Override
    public char[] read(ScanBuffer buffer) {
        int length = getLength(buffer);
        if (length < 0) return null;
        return buffer.getChars(length);
    }

    @Override
    public void write(WriteBuffer buffer, char[] attribute) {
        writeLength(buffer, attribute);
        if (attribute != null) {
            for (char anAttribute : attribute) {
                buffer.putChar(anAttribute);
            }
        }
    }
}
