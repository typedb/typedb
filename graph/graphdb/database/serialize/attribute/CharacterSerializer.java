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

public class CharacterSerializer implements OrderPreservingSerializer<Character> {

    private final ShortSerializer ss = new ShortSerializer();

    @Override
    public Character read(ScanBuffer buffer) {
        final short s = ss.read(buffer);
        return short2char(s);
    }

    @Override
    public void write(WriteBuffer out, Character attribute) {
        ss.write(out, char2short(attribute));
    }

    @Override
    public Character readByteOrder(ScanBuffer buffer) {
        return read(buffer);
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Character attribute) {
        write(buffer,attribute);
    }

    public static short char2short(char c) {
        return (short) (((int) c) + Short.MIN_VALUE);
    }

    public static char short2char(short s) {
        return (char) (((int) s) - Short.MIN_VALUE);
    }

    @Override
    public Character convert(Object value) {
        if (value instanceof String && ((String) value).length() == 1) {
            return ((String) value).charAt(0);
        }
        return null;
    }
}
