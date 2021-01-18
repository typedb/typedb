/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.common.collection;

import grakn.core.common.exception.GraknCheckedException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_STRING_SIZE;
import static java.util.Arrays.copyOfRange;

public class Bytes {

    public static final int SHORT_SIZE = 2;
    public static final int SHORT_UNSIGNED_MAX_VALUE = 65_535; // (2 ^ SHORT_SIZE x 8) - 1
    public static final int INTEGER_SIZE = 4;
    public static final int LONG_SIZE = 8;
    public static final int DOUBLE_SIZE = 8;
    public static final int DATETIME_SIZE = LONG_SIZE;

    public static byte[] join(byte[]... byteArrays) {
        int length = 0;
        for (byte[] array : byteArrays) {
            length += array.length;
        }

        final byte[] joint = new byte[length];
        int pos = 0;
        for (byte[] array : byteArrays) {
            System.arraycopy(array, 0, joint, pos, array.length);
            pos += array.length;
        }

        return joint;
    }

    public static byte[] stripPrefix(byte[] bytes, int prefixLength) {
        return copyOfRange(bytes, prefixLength, bytes.length);
    }

    public static boolean bytesHavePrefix(byte[] bytes, byte[] prefix) {
        if (bytes.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (bytes[i] != prefix[i]) return false;
        }
        return true;
    }

    public static byte[] unsignedShortToBytes(int num) {
        final byte[] bytes = new byte[SHORT_SIZE];
        bytes[1] = (byte) (num);
        bytes[0] = (byte) (num >> 8);
        return bytes;
    }

    public static int unsignedBytesToShort(byte[] bytes) {
        assert bytes.length == SHORT_SIZE;
        return ((bytes[0] << 8) & 0xff00) | (bytes[1] & 0xff);
    }

    public static byte[] shortToSortedBytes(int num) {
        final byte[] bytes = new byte[SHORT_SIZE];
        bytes[1] = (byte) (num);
        bytes[0] = (byte) ((num >> 8) ^ 0x80);
        return bytes;
    }

    public static short sortedBytesToShort(byte[] bytes) {
        assert bytes.length == SHORT_SIZE;
        bytes[0] = (byte) (bytes[0] ^ 0x80);
        return ByteBuffer.wrap(bytes).getShort();
    }

    public static byte[] integerToSortedBytes(int num) {
        final byte[] bytes = new byte[INTEGER_SIZE];
        bytes[3] = (byte) (num);
        bytes[2] = (byte) (num >>= 8);
        bytes[1] = (byte) (num >>= 8);
        bytes[0] = (byte) ((num >> 8) ^ 0x80);
        return bytes;
    }

    public static long sortedBytesToInteger(byte[] bytes) {
        assert bytes.length == INTEGER_SIZE;
        bytes[0] = (byte) (bytes[0] ^ 0x80);
        return ByteBuffer.wrap(bytes).getInt();
    }

    public static byte[] longToSortedBytes(long num) {
        final byte[] bytes = new byte[LONG_SIZE];
        bytes[7] = (byte) (num);
        bytes[6] = (byte) (num >>= 8);
        bytes[5] = (byte) (num >>= 8);
        bytes[4] = (byte) (num >>= 8);
        bytes[3] = (byte) (num >>= 8);
        bytes[2] = (byte) (num >>= 8);
        bytes[1] = (byte) (num >>= 8);
        bytes[0] = (byte) ((num >> 8) ^ 0x80);
        return bytes;
    }

    public static long sortedBytesToLong(byte[] bytes) {
        assert bytes.length == LONG_SIZE;
        bytes[0] = (byte) (bytes[0] ^ 0x80);
        return ByteBuffer.wrap(bytes).getLong();
    }

    public static byte[] longToBytes(long num) {
        ByteBuffer buf = ByteBuffer.allocate(LONG_SIZE).order(ByteOrder.nativeOrder());
        buf.putLong(num);
        return buf.array();
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).order(ByteOrder.nativeOrder());
        buf.put(bytes);
        buf.flip();
        return buf.getLong();
    }

    /**
     * Convert {@code double} to lexicographically sorted bytes.
     *
     * We need to implement a custom byte representation of doubles. The bytes
     * need to be lexicographically sortable in the same order as the numerical
     * values of themselves. I.e. The bytes of -10 need to come before -1, -1
     * before 0, 0 before 1, and 1 before 10, and so on. This is not true with
     * the (default) 2's complement byte representation of doubles.
     *
     * We need to XOR all positive numbers with 0x8000... and XOR negative
     * numbers with 0xffff... This should flip the sign bit on both (so negative
     * numbers go first), and then reverse the ordering on negative numbers.
     *
     * @param value the {@code double} value to convert
     * @return the sorted byte representation of the {@code double} value
     */
    public static byte[] doubleToSortedBytes(double value) {
        final byte[] bytes = ByteBuffer.allocate(DOUBLE_SIZE).putDouble(value).array();
        if (value >= 0) {
            bytes[0] = (byte) (bytes[0] ^ 0x80);
        } else {
            for (int i = 0; i < DOUBLE_SIZE; i++) {
                bytes[i] = (byte) (bytes[i] ^ 0xff);
            }
        }
        return bytes;
    }

    public static double sortedBytesToDouble(byte[] bytes) {
        assert bytes.length == DOUBLE_SIZE;
        if ((bytes[0] & 0x80) == 0x80) {
            bytes[0] = (byte) (bytes[0] ^ 0x80);
        } else {
            for (int i = 0; i < DOUBLE_SIZE; i++) {
                bytes[i] = (byte) (bytes[i] ^ 0xff);
            }
        }
        return ByteBuffer.wrap(bytes).getDouble();
    }

    public static byte[] stringToBytes(String value, Charset encoding) throws GraknCheckedException {
        final byte[] bytes = value.getBytes(encoding);
        if (bytes.length > SHORT_UNSIGNED_MAX_VALUE) {
            throw GraknCheckedException.of(ILLEGAL_STRING_SIZE, SHORT_UNSIGNED_MAX_VALUE);
        }
        return join(unsignedShortToBytes(bytes.length), bytes);
    }

    public static String bytesToString(byte[] bytes, Charset encoding) {
        int stringLength = unsignedBytesToShort(new byte[]{bytes[0], bytes[1]});
        final byte[] x = copyOfRange(bytes, SHORT_SIZE, SHORT_SIZE + stringLength);
        return new String(x, encoding);
    }

    public static byte booleanToByte(boolean value) {
        return (byte) (value ? 1 : 0);
    }

    public static Boolean byteToBoolean(byte aByte) {
        return aByte == 1;
    }

    public static byte[] dateTimeToBytes(java.time.LocalDateTime value, ZoneId timeZoneID) {
        return longToSortedBytes(value.atZone(timeZoneID).toInstant().toEpochMilli());
    }

    public static java.time.LocalDateTime bytesToDateTime(byte[] bytes, ZoneId timeZoneID) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(sortedBytesToLong(bytes)), timeZoneID);
    }

    public static byte[] uuidToBytes(UUID uuid) {
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return buffer.array();
    }

    public static UUID bytesToUUID(byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final long firstLong = buffer.getLong();
        final long secondLong = buffer.getLong();
        return new UUID(firstLong, secondLong);
    }

    public static boolean arrayContains(byte[] container, int from, byte[] contained) {
        if ((container.length - from) > contained.length) return false;
        for (int i = 0; i < contained.length; i++) {
            if (container[from + i] != contained[i]) return false;
        }
        return true;
    }

    public static byte checkedCast(int value) {
        assert (value & 0xFFFFFF00) == 0;
        return (byte) value;
    }
}
