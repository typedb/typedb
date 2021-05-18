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

public abstract class ByteArray implements Comparable<ByteArray> {

    final byte[] array;

    private ByteArray(byte[] array) {
        this.array = array;
    }

    public static ByteArray.Base of(byte[] array) {
        return new ByteArray.Base(array);
    }

    public static ByteArray empty() {
        return new ByteArray.Base(new byte[]{});
    }

    public abstract byte[] getBytes();

    public abstract byte[] cloneBytes();

    public abstract boolean isEmpty();

    public abstract int length();

    public abstract byte get(int index);

    /**
     * @param from - copy start index, inclusive
     * @return - copy of byte sub array, starting at from to the end, inclusive
     */
    public ByteArray copyRange(int from) {
        return copyRange(from, length());
    }

    /**
     * @param from - copy start index, inclusive
     * @param to   - copy end index, exclusive
     * @return - copy of byte sub-array
     */
    public abstract ByteArray.Base copyRange(int from, int to);

    public ByteArray.View view(int from) {
        return view(from, length());
    }

    public abstract ByteArray.View view(int from, int to);

    /**
     * WARNING: not efficient if used to join VIEW arrays
     *
     * @param arrays - list of byte arrays to copy into a larger array
     * @return - new merged byte array
     */
    public static Base join(ByteArray... arrays) {
        int length = 0;
        for (ByteArray array : arrays) {
            length += array.length();
        }

        byte[] joint = new byte[length];
        int pos = 0;
        for (ByteArray array : arrays) {
            array.copyInto(joint, pos);
            pos += array.length();
        }

        return new Base(joint);
    }

    abstract void copyInto(byte[] destination, int pos);

    public static ByteArray.Base encodeString(String string, Charset encoding) {
        return of(string.getBytes(encoding));
    }

    public abstract String decodeString();

    public abstract String decodeString(Charset encoding);

    public static ByteArray.Base encodeUnsignedShort(int num) {
        byte[] bytes = new byte[SHORT_SIZE];
        bytes[1] = (byte) (num);
        bytes[0] = (byte) (num >> 8);
        return of(bytes);
    }

    public abstract int decodeUnsignedShort();

    public static ByteArray encodeLong(long num) {
        return of(ByteBuffer.allocate(LONG_SIZE).order(LITTLE_ENDIAN).putLong(num).array());
    }

    public abstract long decodeLong();

    public static ByteArray encodeInt(int num) {
        return of(ByteBuffer.allocate(INTEGER_SIZE).order(LITTLE_ENDIAN).putInt(num).array());
    }

    public abstract int decodeInt();

    public static ByteArray.Base encodeUUID(UUID uuid) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return of(buffer.array());
    }

    public abstract UUID decodeUUID();

    public abstract boolean hasPrefix(ByteArray prefix);

    @Override
    public int compareTo(ByteArray that) {
        if (that instanceof Base) return compareToBase((Base) that);
        else return compareToView((View) that);
    }

    abstract int compareToView(View that);

    abstract int compareToBase(Base that);

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (o.getClass() == Base.class) return equalsBase((Base) o);
        else if (o.getClass() == View.class) return equalsView((View) o);
        else return false;
    }

    abstract boolean equalsView(View o);

    abstract boolean equalsBase(Base o);

    @Override
    public abstract int hashCode();

    @Override
    public String toString() {
        return Arrays.toString(getBytes());
    }

    public String toHexString() {
        return Bytes.bytesToHexString(getBytes());
    }

    public static ByteArray fromHexString(String string) {
        return of(Bytes.hexStringToBytes(string));
    }

    public static class Base extends ByteArray {

        public Base(byte[] array) {
            super(array);
        }

        @Override
        public byte[] getBytes() {
            return array;
        }

        @Override
        public byte[] cloneBytes() {
            return Arrays.copyOf(array, array.length);
        }

        @Override
        public boolean isEmpty() {
            return array.length == 0;
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public byte get(int index) {
            return array[index];
        }

        @Override
        public ByteArray.Base copyRange(int from, int to) {
            if (from >= array.length || to > array.length) throw new IndexOutOfBoundsException();
            return new Base(Arrays.copyOfRange(array, from, to));
        }

        @Override
        public ByteArray.View view(int from, int to) {
            if (from >= array.length || to > array.length) throw new IndexOutOfBoundsException();
            return new ByteArray.View(array, from, to - from);
        }

        @Override
        void copyInto(byte[] destination, int pos) {
            assert pos + array.length <= destination.length;
            System.arraycopy(array, 0, destination, pos, array.length);
        }

        @Override
        public String decodeString() {
            return new String(array);
        }

        @Override
        public String decodeString(Charset encoding) {
            return new String(array, encoding);
        }

        @Override
        public UUID decodeUUID() {
            ByteBuffer buffer = ByteBuffer.wrap(array);
            long firstLong = buffer.getLong();
            long secondLong = buffer.getLong();
            return new UUID(firstLong, secondLong);
        }

        @Override
        public int decodeUnsignedShort() {
            assert array.length == SHORT_SIZE;
            return ((array[0] << 8) & 0xff00) | (array[1] & 0xff);
        }

        @Override
        public long decodeLong() {
            assert array.length == LONG_SIZE;
            return ByteBuffer.wrap(array).order(LITTLE_ENDIAN).getLong();
        }

        @Override
        public int decodeInt() {
            assert array.length == INTEGER_SIZE;
            return ByteBuffer.wrap(array).order(LITTLE_ENDIAN).getInt();
        }

        @Override
        public boolean hasPrefix(ByteArray prefix) {
            if (array.length < prefix.length()) return false;
            for (int i = 0; i < prefix.length(); i++) {
                if (array[i] != prefix.get(i)) return false;
            }
            return true;
        }

        @Override
        int compareToBase(Base that) {
            int n = Math.min(array.length, that.array.length);
            for (int i = 0; i < n; i++) {
                int cmp = Byte.compare(array[i], that.array[i]);
                if (cmp != 0) return cmp;
            }
            return Integer.compare(array.length, that.array.length);
        }

        @Override
        int compareToView(View that) {
            int n = Math.min(array.length, that.length);
            for (int i = 0; i < n; i++) {
                int cmp = Byte.compare(array[i], that.array[i + that.start]);
                if (cmp != 0) return cmp;
            }
            return Integer.compare(array.length, that.length);
        }

        @Override
        public boolean equalsBase(Base o) {
            return Arrays.equals(array, o.array);
        }

        @Override
        boolean equalsView(View o) {
            if (array.length != o.length()) return false;
            for (int i = 0; i < array.length; i++) {
                if (array[i] != o.array[i + o.start]) return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int h = 1;
            for (final byte b : array) {
                h = 31 * h + (int) b;
            }
            return h;
        }
    }

    public static class View extends ByteArray {

        private final int start;
        private final int length;
        private byte[] arrayCache;

        private View(byte[] array, int start, int length) {
            super(array);
            this.start = start;
            this.length = length;
        }

        @Override
        public byte[] getBytes() {
            if (arrayCache == null) arrayCache = Arrays.copyOfRange(array, start, start + length);
            return arrayCache;
        }

        @Override
        public byte[] cloneBytes() {
            return Arrays.copyOfRange(array, start, start + length);
        }

        @Override
        public boolean isEmpty() {
            return length == 0;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public byte get(int index) {
            if (index >= length) throw new ArrayIndexOutOfBoundsException();
            return array[index + start];
        }

        @Override
        public ByteArray.Base copyRange(int from, int to) {
            if (from >= length || to > length) throw new ArrayIndexOutOfBoundsException();
            return new ByteArray.Base(Arrays.copyOfRange(array, start + from, start + (to - from)));
        }

        @Override
        public ByteArray.View view(int from, int to) {
            if (from >= length || to > length) throw new ArrayIndexOutOfBoundsException();
            return new View(array, start + from, to - from);
        }

        @Override
        void copyInto(byte[] destination, int pos) {
            assert pos + length <= destination.length;
            System.arraycopy(array, start, destination, pos, length);
        }

        @Override
        public String decodeString() {
            return new String(array, start, length);
        }

        @Override
        public String decodeString(Charset encoding) {
            return new String(array, start, length, encoding);
        }

        @Override
        public int decodeUnsignedShort() {
            assert length == SHORT_SIZE;
            return ((array[start] << 8) & 0xff00) | (array[start + 1] & 0xff);
        }

        @Override
        public long decodeLong() {
            assert length == LONG_SIZE;
            return ByteBuffer.wrap(array, start, length).order(LITTLE_ENDIAN).getLong();
        }

        @Override
        public int decodeInt() {
            assert length == INTEGER_SIZE;
            return ByteBuffer.wrap(array, start, length).order(LITTLE_ENDIAN).getInt();
        }

        @Override
        public UUID decodeUUID() {
            ByteBuffer buffer = ByteBuffer.wrap(array, start, length);
            long firstLong = buffer.getLong();
            long secondLong = buffer.getLong();
            return new UUID(firstLong, secondLong);
        }

        @Override
        public boolean hasPrefix(ByteArray prefix) {
            if (length < prefix.length()) return false;
            for (int i = 0; i < prefix.length(); i++) {
                if (array[i + start] != prefix.get(i)) return false;
            }
            return true;
        }

        @Override
        int compareToBase(Base that) {
            int n = Math.min(length, that.array.length);
            for (int i = 0; i < n; i++) {
                int cmp = Byte.compare(array[i + start], that.array[i]);
                if (cmp != 0) return cmp;
            }
            return Integer.compare(length, that.array.length);
        }

        @Override
        int compareToView(View that) {
            int n = Math.min(length, that.length);
            for (int i = 0; i < n; i++) {
                int cmp = Byte.compare(array[i + start], that.array[i + that.start]);
                if (cmp != 0) return cmp;
            }
            return Integer.compare(length, that.length);
        }

        @Override
        boolean equalsBase(Base o) {
            if (length != o.length()) return false;
            for (int i = 0; i < length; i++) {
                if (array[i + start] != o.array[i]) return false;
            }
            return true;
        }

        @Override
        boolean equalsView(View o) {
            if (length != o.length()) return false;
            for (int i = 0; i < length; i++) {
                if (array[i + start] != o.array[i + o.start]) return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int h = 1;
            for (int i = start; i < start + length; i++) {
                h = 31 * h + (int) array[i];
            }
            return h;
        }
    }
}
