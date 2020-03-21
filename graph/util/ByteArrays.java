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

package hypergraph.graph.util;

public class ByteArrays {

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

    public static byte[] toShortBytes(int num) {
        return new byte[] {
                (byte) (num >> 8),
                (byte) (num)
        };
    }

    public static byte[] toLongBytes(long num) {
        return new byte[] {
                (byte) (num >> 56),
                (byte) (num >> 48),
                (byte) (num >> 40),
                (byte) (num >> 32),
                (byte) (num >> 24),
                (byte) (num >> 16),
                (byte) (num >> 8),
                (byte) (num),
        };
    }
}
