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

package grakn.core.graph.diskstorage.util;

import grakn.core.graph.diskstorage.ReadBuffer;

/**
 * Implementation of ReadBuffer against a byte array.
 * <p>
 * Note, that the position does not impact the state of the object. Meaning, equals, hashcode,
 * and compare ignore the position.
 *
 */

public class ReadArrayBuffer extends StaticArrayBuffer implements ReadBuffer {

    public ReadArrayBuffer(byte[] array) {
        super(array);
    }

    ReadArrayBuffer(StaticArrayBuffer buffer) {
        super(buffer);
    }

    protected ReadArrayBuffer(byte[] array, int limit) {
        super(array, 0, limit);
    }

    @Override
    void reset(int newOffset, int newLimit) {
        position = 0;
        super.reset(newOffset, newLimit);
    }

    /*
    ############ IDENTICAL CODE #############
     */
    private transient int position = 0;

    private int updatePos(int update) {
        int pos = position;
        position += update;
        return pos;
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public boolean hasRemaining() {
        return position < length();
    }

    @Override
    public void movePositionTo(int newPosition) {
        position = newPosition;
    }

    @Override
    public byte getByte() {
        return getByte(updatePos(1));
    }

    @Override
    public boolean getBoolean() {
        return getBoolean(updatePos(1));
    }

    @Override
    public short getShort() {
        return getShort(updatePos(2));
    }

    @Override
    public int getInt() {
        return getInt(updatePos(4));
    }

    @Override
    public long getLong() {
        return getLong(updatePos(8));
    }

    @Override
    public char getChar() {
        return getChar(updatePos(2));
    }

    @Override
    public float getFloat() {
        return getFloat(updatePos(4));
    }

    @Override
    public double getDouble() {
        return getDouble(updatePos(8));
    }

    //------

    public byte[] getBytes(int length) {
        byte[] result = super.getBytes(position, length);
        position += length * BYTE_LEN;
        return result;
    }

    public short[] getShorts(int length) {
        short[] result = super.getShorts(position, length);
        position += length * SHORT_LEN;
        return result;
    }

    public int[] getInts(int length) {
        int[] result = super.getInts(position, length);
        position += length * INT_LEN;
        return result;
    }

    public long[] getLongs(int length) {
        long[] result = super.getLongs(position, length);
        position += length * LONG_LEN;
        return result;
    }

    public char[] getChars(int length) {
        char[] result = super.getChars(position, length);
        position += length * CHAR_LEN;
        return result;
    }

    public float[] getFloats(int length) {
        float[] result = super.getFloats(position, length);
        position += length * FLOAT_LEN;
        return result;
    }

    public double[] getDoubles(int length) {
        double[] result = super.getDoubles(position, length);
        position += length * DOUBLE_LEN;
        return result;
    }

    @Override
    public <T> T asRelative(Factory<T> factory) {
        if (position == 0) return as(factory);
        else {
            return as((array, offset, limit) -> factory.get(array, offset + position, limit));
        }
    }

    @Override
    public ReadBuffer subrange(int length, boolean invert) {
        return super.subrange(position, length, invert).asReadBuffer();
    }


}
