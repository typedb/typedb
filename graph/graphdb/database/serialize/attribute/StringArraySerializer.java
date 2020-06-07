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

public class StringArraySerializer extends ArraySerializer implements AttributeSerializer<String[]> {

    private static final StringSerializer stringSerializer = new StringSerializer();

    @Override
    public String[] convert(Object value) {
        return convertInternal(value, null, String.class);
    }

    @Override
    protected Object getArray(int length) {
        return new String[length];
    }

    @Override
    protected void setArray(Object array, int pos, Object value) {
        Array.set(array, pos, value);
    }

    //############### Serialization ###################

    @Override
    public String[] read(ScanBuffer buffer) {
        int length = getLength(buffer);
        if (length < 0) return null;
        String[] result = new String[length];
        for (int i = 0; i < length; i++) {
            result[i] = stringSerializer.read(buffer);
        }
        return result;
    }

    @Override
    public void write(WriteBuffer buffer, String[] attribute) {
        writeLength(buffer, attribute);
        if (attribute != null) {
            for (String anAttribute : attribute) {
                stringSerializer.write(buffer, anAttribute);
            }
        }
    }
}
