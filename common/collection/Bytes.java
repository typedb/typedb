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

package com.vaticle.typedb.core.common.collection;

public class Bytes {

    public interface ByteComparable<T extends ByteComparable<T>> extends Comparable<T> {

        ByteArray getBytes();

        @Override
        default int compareTo(T o) {
            return getBytes().compareTo(o.getBytes());
        }

    }

    public static final int SHORT_SIZE = 2;
    public static final int SHORT_UNSIGNED_MAX_VALUE = 65_535; // (2 ^ SHORT_SIZE x 8) - 1
    public static final int INTEGER_SIZE = 4;
    public static final int LONG_SIZE = 8;
    public static final int DOUBLE_SIZE = 8;
    public static final int DATETIME_SIZE = LONG_SIZE;

    public static byte booleanToByte(boolean value) {
        return (byte) (value ? 1 : 0);
    }

    public static Boolean byteToBoolean(byte aByte) {
        return aByte == 1;
    }

    public static byte signedByte(int value) {
        assert value >= -128 && value <= 127;
        return (byte) value;
    }

    public static byte unsignedByte(int value) {
        assert value >= 0 && value <= 255;
        return (byte) (value & 0xff);
    }
}
