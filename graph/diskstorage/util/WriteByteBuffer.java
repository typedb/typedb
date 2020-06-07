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

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;

import java.nio.ByteBuffer;

import static grakn.core.graph.diskstorage.util.StaticArrayBuffer.BYTE_LEN;
import static grakn.core.graph.diskstorage.util.StaticArrayBuffer.CHAR_LEN;
import static grakn.core.graph.diskstorage.util.StaticArrayBuffer.DOUBLE_LEN;
import static grakn.core.graph.diskstorage.util.StaticArrayBuffer.FLOAT_LEN;
import static grakn.core.graph.diskstorage.util.StaticArrayBuffer.INT_LEN;
import static grakn.core.graph.diskstorage.util.StaticArrayBuffer.LONG_LEN;
import static grakn.core.graph.diskstorage.util.StaticArrayBuffer.SHORT_LEN;


public class WriteByteBuffer implements WriteBuffer {

    private static final int DEFAULT_CAPACITY = 64;
    private static final int MAX_BUFFER_CAPACITY = 128 * 1024 * 1024; //128 MB

    private ByteBuffer buffer;

    public WriteByteBuffer() {
        this(DEFAULT_CAPACITY);
    }

    public WriteByteBuffer(int capacity) {
        Preconditions.checkArgument(capacity <= MAX_BUFFER_CAPACITY, "Capacity exceeds max buffer capacity: %s", MAX_BUFFER_CAPACITY);
        buffer = ByteBuffer.allocate(capacity);
    }

    private void require(int size) {
        if (buffer.capacity() - buffer.position() < size) {
            //Need to resize
            int newCapacity = buffer.position() + size + buffer.capacity(); //extra capacity as buffer
            Preconditions.checkArgument(newCapacity <= MAX_BUFFER_CAPACITY, "Capacity exceeds max buffer capacity: %s", MAX_BUFFER_CAPACITY);
            ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);
            buffer.flip();
            newBuffer.put(buffer);
            buffer = newBuffer;
        }
    }

    @Override
    public WriteBuffer putLong(long val) {
        require(LONG_LEN);
        buffer.putLong(val);
        return this;
    }

    @Override
    public WriteBuffer putInt(int val) {
        require(INT_LEN);
        buffer.putInt(val);
        return this;
    }

    @Override
    public WriteBuffer putShort(short val) {
        require(SHORT_LEN);
        buffer.putShort(val);
        return this;
    }

    @Override
    public WriteBuffer putBoolean(boolean val) {
        return putByte((byte) (val ? 1 : 0));
    }

    @Override
    public WriteBuffer putByte(byte val) {
        require(BYTE_LEN);
        buffer.put(val);
        return this;
    }

    @Override
    public WriteBuffer putBytes(byte[] val) {
        require(BYTE_LEN * val.length);
        buffer.put(val);
        return this;
    }

    @Override
    public WriteBuffer putBytes(StaticBuffer val) {
        require(BYTE_LEN * val.length());
        val.as((array, offset, limit) -> {
            buffer.put(array, offset, val.length());
            return Boolean.TRUE;
        });
        return this;
    }

    @Override
    public WriteBuffer putChar(char val) {
        require(CHAR_LEN);
        buffer.putChar(val);
        return this;
    }

    @Override
    public WriteBuffer putFloat(float val) {
        require(FLOAT_LEN);
        buffer.putFloat(val);
        return this;
    }

    @Override
    public WriteBuffer putDouble(double val) {
        require(DOUBLE_LEN);
        buffer.putDouble(val);
        return this;
    }

    @Override
    public int getPosition() {
        return buffer.position();
    }

    @Override
    public StaticBuffer getStaticBuffer() {
        return getStaticBufferFlipBytes(0, 0);
    }

    @Override
    public StaticBuffer getStaticBufferFlipBytes(int from, int to) {
        ByteBuffer b = buffer.duplicate();
        b.flip();
        Preconditions.checkArgument(from >= 0 && from <= to);
        Preconditions.checkArgument(to <= b.limit());
        for (int i = from; i < to; i++) b.put(i, (byte) ~b.get(i));
        return StaticArrayBuffer.of(b);
    }
}
