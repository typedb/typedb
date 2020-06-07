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

import java.lang.reflect.Constructor;

/**
 * Utility methods for hexadecimal representation of bytes
 */
public class Hex {
    private static final Constructor<String> stringConstructor = getStringProtectedConstructor(int.class, int.class, char[].class);

    // package protected for use by BufferUtil. Do not modify this array !!
    static final char[] byteToChar = new char[16];

    static {
        for (int i = 0; i < 16; ++i) {
            byteToChar[i] = Integer.toHexString(i).charAt(0);
        }
    }

    public static String bytesToHex(byte... bytes) {
        char[] c = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int byteAsInteger = bytes[i];
            c[i * 2] = byteToChar[(byteAsInteger & 0xf0) >> 4];
            c[1 + i * 2] = byteToChar[byteAsInteger & 0x0f];
        }

        return wrapCharArray(c);
    }

    /**
     * Create a String from a char array with zero-copy (if available), using reflection to access a package-protected constructor of String.
     */
    public static String wrapCharArray(char[] c) {
        if (c == null) {
            return null;
        }
        String s = null;

        if (stringConstructor != null) {
            try {
                s = stringConstructor.newInstance(0, c.length, c);
            } catch (Exception e) {
                // Swallowing as we'll just use a copying constructor
            }
        }
        return s == null ? new String(c) : s;
    }

    /**
     * Used to get access to protected/private constructor of the specified class
     *
     * @param paramTypes - types of the constructor parameters
     * @return Constructor if successful, null if the constructor cannot be
     * accessed
     */
    private static Constructor getStringProtectedConstructor(Class... paramTypes) {
        Constructor c;
        try {
            c = ((Class) String.class).getDeclaredConstructor(paramTypes);
            c.setAccessible(true);
            return c;
        } catch (Exception e) {
            return null;
        }
    }
}
