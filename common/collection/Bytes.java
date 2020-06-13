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
 *
 */

package hypergraph.common.collection;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;

public class Bytes {

    public static final int SHORT_SIZE = 2;
    public static final int INTEGER_SIZE = 4;
    public static final int LONG_SIZE = 8;
    public static final int DOUBLE_SIZE = 8;
    public static final int DATETIME_SIZE = LONG_SIZE;

    public static byte[] join(byte[]... byteArrays) {
        int length = 0;
        for (byte[] array : byteArrays) {
            length += array.length;
        }

        byte[] joint = new byte[length];
        int pos = 0;
        for (byte[] array : byteArrays) {
            System.arraycopy(array, 0, joint, pos, array.length);
            pos += array.length;
        }

        return joint;
    }

    public static boolean bytesHavePrefix(byte[] bytes, byte[] prefix) {
        if (bytes.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (bytes[i] != prefix[i]) return false;
        }
        return true;
    }

    public static byte[] shortToBytes(int num) {
        byte[] bytes = new byte[SHORT_SIZE];
        bytes[1] = (byte) (num);
        bytes[0] = (byte) (num >> 8);
        return bytes;
    }

    public static Short bytesToShort(byte[] bytes) {
        assert bytes.length == SHORT_SIZE;
        return ByteBuffer.wrap(bytes).getShort();
    }

    public static byte[] integerToBytes(int num) {
        byte[] bytes = new byte[INTEGER_SIZE];
        bytes[3] = (byte) (num);
        bytes[2] = (byte) (num >>= 8);
        bytes[1] = (byte) (num >>= 8);
        bytes[0] = (byte) (num >> 8);
        return bytes;
    }

    public static byte[] longToBytes(long num) {
        byte[] bytes = new byte[LONG_SIZE];
        bytes[7] = (byte) (num);
        bytes[6] = (byte) (num >>= 8);
        bytes[5] = (byte) (num >>= 8);
        bytes[4] = (byte) (num >>= 8);
        bytes[3] = (byte) (num >>= 8);
        bytes[2] = (byte) (num >>= 8);
        bytes[1] = (byte) (num >>= 8);
        bytes[0] = (byte) (num >> 8);
        return bytes;
    }

    public static long bytesToLong(byte[] bytes) {
        assert bytes.length == LONG_SIZE;
        return ByteBuffer.wrap(bytes).getLong();
    }

    public static byte[] doubleToBytes(double value) {
        return ByteBuffer.allocate(DOUBLE_SIZE).putDouble(value).array();
        // TODO: We need to implement a custom byte representation of doubles.
        //       The bytes need to be lexicographically sortable in the same
        //       order as the numerical values of themselves.
        //       I.e. The bytes of -10 need to come before -1, -1 before 0,
        //       0 before 1, and 1 before 10, and so on. This is not true with
        //       the (default) 2's complement byte representation of doubles.
        //       We need to XOR all positive numbers with 0x8000... and XOR
        //       negative numbers with 0xffff... This should flip the sign bit
        //       on both (so negative numbers go first), and then reverse the
        //       ordering on negative numbers.
    }

    public static double bytesToDouble(byte[] bytes) {
        assert bytes.length == DOUBLE_SIZE;
        return ByteBuffer.wrap(bytes).getDouble();
    }

    public static byte[] stringToBytes(String value, Charset encoding) {
        byte[] bytes = value.getBytes(encoding);
        return join(new byte[]{(byte) bytes.length}, bytes);
    }

    public static String bytesToString(byte[] bytes, Charset encoding) {
        byte[] x = Arrays.copyOfRange(bytes, 1, 1 + bytes[0]);
        return new String(x, encoding);
    }

    public static byte booleanToByte(boolean value) {
        return (byte) (value ? 1 : 0);
    }

    public static Boolean byteToBoolean(byte aByte) {
        return aByte == 1;
    }

    public static byte[] dateTimeToBytes(java.time.LocalDateTime value, ZoneId timeZoneID) {
        return longToBytes(value.atZone(timeZoneID).toInstant().toEpochMilli());
    }

    public static java.time.LocalDateTime bytesToDateTime(byte[] bytes, ZoneId timeZoneID) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(bytesToLong(bytes)), timeZoneID);
    }
}
