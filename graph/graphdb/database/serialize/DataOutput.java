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

package grakn.core.graph.graphdb.database.serialize;

import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;

public interface DataOutput extends WriteBuffer {

    @Override
    DataOutput putLong(long val);

    @Override
    DataOutput putInt(int val);

    @Override
    DataOutput putShort(short val);

    @Override
    DataOutput putByte(byte val);

    @Override
    DataOutput putBytes(byte[] val);

    @Override
    DataOutput putBytes(StaticBuffer val);

    @Override
    DataOutput putChar(char val);

    @Override
    DataOutput putFloat(float val);

    @Override
    DataOutput putDouble(double val);

    DataOutput writeObject(Object object, Class<?> type);

    DataOutput writeObjectByteOrder(Object object, Class<?> type);

    DataOutput writeObjectNotNull(Object object);

    DataOutput writeClassAndObject(Object object);

}
