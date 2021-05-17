/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.common.util;

import com.vaticle.typedb.common.collection.Bytes;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.UUID;

import static com.vaticle.typedb.core.common.collection.Bytes.INTEGER_SIZE;
import static com.vaticle.typedb.core.common.collection.Bytes.LONG_SIZE;
import static com.vaticle.typedb.core.common.collection.Bytes.SHORT_SIZE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class ByteArray implements Comparable<ByteArray> {

    private final byte[] array;

    private ByteArray(byte[] array) {
        this.array = array;
    }

    public static ByteArray of(byte[] array) {
        return new ByteArray(array);
    }

    public byte[] getBytes() {
        return array;
    }

    public boolean isEmpty() {
        return array.length == 0;
    }

    public int length() {
        return array.length;
    }

    public byte get(int index) {
        return array[index];
    }

    public static ByteArray join(ByteArray... arrays) {
        int length = 0;
        for (ByteArray array : arrays) {
            length += array.array.length;
        }

        byte[] joint = new byte[length];
        int pos = 0;
        for (ByteArray array : arrays) {
            System.arraycopy(array.array, 0, joint, pos, array.array.length);
            pos += array.array.length;
        }

        return new ByteArray(joint);
    }

    /**
     * @param from - copy start index, inclusive
     * @return - copy of byte sub array, starting at from to the end, inclusive
     */
    public ByteArray copyRange(int from) {
        return copyRange(from, array.length);
    }

    /**
     * @param from - copy start index, inclusive
     * @param to - copy end index, exclusive
     * @return - copy of byte sub-array
     */
    public ByteArray copyRange(int from, int to) {
        return new ByteArray(Arrays.copyOfRange(array, from, to));
    }

    public ByteArray stripPrefix(int prefixLength) {
        return copyRange(prefixLength);
    }

    public static ByteArray encodeString(String string, Charset encoding) {
        return of(string.getBytes(encoding));
    }

    public String decodeString() {
        return new String(array);
    }

    public String decodeString(Charset encoding) {
        return new String(array, encoding);
    }

    public static ByteArray encodeUUID(UUID uuid) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return of(buffer.array());
    }

    public UUID decodeUUID() {
        ByteBuffer buffer = ByteBuffer.wrap(array);
        long firstLong = buffer.getLong();
        long secondLong = buffer.getLong();
        return new UUID(firstLong, secondLong);
    }

    public static ByteArray encodeUnsignedShort(int num) {
        byte[] bytes = new byte[SHORT_SIZE];
        bytes[1] = (byte) (num);
        bytes[0] = (byte) (num >> 8);
        return of(bytes);
    }

    public int decodeUnsignedShort() {
        assert array.length == SHORT_SIZE;
        return ((array[0] << 8) & 0xff00) | (array[1] & 0xff);
    }

    public static ByteArray encodeLong(long num) {
        return of(ByteBuffer.allocate(LONG_SIZE).order(LITTLE_ENDIAN).putLong(num).array());
    }

    public long decodeLong() {
        assert array.length == LONG_SIZE;
        return ByteBuffer.wrap(array).order(LITTLE_ENDIAN).getLong();
    }

    public static ByteArray encodeInt(int num) {
        return of(ByteBuffer.allocate(INTEGER_SIZE).order(LITTLE_ENDIAN).putInt(num).array());
    }

    public int decodeInt() {
        assert array.length == INTEGER_SIZE;
        return ByteBuffer.wrap(array).order(LITTLE_ENDIAN).getInt();
    }

    public boolean hasPrefix(ByteArray prefix) {
        if (array.length < prefix.array.length) return false;
        for (int i = 0; i < prefix.array.length; i++) {
            if (array[i] != prefix.array[i]) return false;
        }
        return true;
    }

    @Override
    public int compareTo(ByteArray that) {
        int n = Math.min(this.array.length, that.array.length);
        for (int i = 0; i < n; i++) {
            int cmp = Byte.compare(array[i], that.array[i]);
            if (cmp != 0) return cmp;
        }
        return Integer.compare(this.array.length, that.array.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ByteArray byteArray = (ByteArray) o;
        return Arrays.equals(array, byteArray.array);
    }

    @Override
    public int hashCode() {
        // TODO may want to cache, is it a worthwhile memory tradeoff?
        return Arrays.hashCode(array);
    }

    @Override
    public String toString() {
        return Arrays.toString(array);
    }

    public String toHexString() {
        return Bytes.bytesToHexString(array);
    }

    public static ByteArray fromHexString(String string) {
        return of(Bytes.hexStringToBytes(string));
    }
}
