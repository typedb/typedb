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

package grakn.core.graph.diskstorage;

import grakn.core.graph.diskstorage.util.StaticArrayBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A Buffer that only allows static access. This Buffer is immutable if
 * any returned byte array or ByteBuffer is not mutated.
 */
public interface StaticBuffer extends Comparable<StaticBuffer> {

    int length();

    byte getByte(int position);

    boolean getBoolean(int position);

    short getShort(int position);

    int getInt(int position);

    long getLong(int position);

    char getChar(int position);

    float getFloat(int position);

    double getDouble(int position);

    byte[] getBytes(int position, int length);

    short[] getShorts(int position, int length);

    int[] getInts(int position, int length);

    long[] getLongs(int position, int length);

    char[] getChars(int position, int length);

    float[] getFloats(int position, int length);

    double[] getDoubles(int position, int length);

    StaticBuffer subrange(int position, int length);

    StaticBuffer subrange(int position, int length, boolean invert);

    ReadBuffer asReadBuffer();

    <T> T as(Factory<T> factory);

    //Convenience method
    ByteBuffer asByteBuffer();

    interface Factory<T> {

        T get(byte[] array, int offset, int limit);

    }

    Factory<byte[]> ARRAY_FACTORY = (array, offset, limit) -> {
        if (offset == 0 && limit == array.length) return array;
        else return Arrays.copyOfRange(array, offset, limit);
    };

    Factory<ByteBuffer> BB_FACTORY = (array, offset, limit) -> ByteBuffer.wrap(array, offset, limit - offset);

    Factory<StaticBuffer> STATIC_FACTORY = StaticArrayBuffer::new;

}
