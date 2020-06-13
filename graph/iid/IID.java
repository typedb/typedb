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

package hypergraph.graph.iid;

import java.util.Arrays;

public abstract class IID {

    // TODO: convert HEX_ARRAY to byte[] once upgraded to Java 9+
    private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();
    protected final byte[] bytes;
    protected String readableString; // for debugging
    private String hexString;
    private int hash = 0;

    IID(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] bytes() {
        return bytes;
    }

    public String toHexString() {
        if (hexString == null) {
            char[] hexChars = new char[bytes.length * 2];
            for (int j = 0; j < bytes.length; j++) {
                int v = bytes[j] & 0xFF;
                hexChars[j * 2] = HEX_ARRAY[v >>> 4];
                hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
            }
            hexString = new String(hexChars);
            // TODO if hexChars is a byte[]: return new String(hexChars, StandardCharsets.UTF_8);
        }
        return hexString;
    }

    @Override
    public abstract String toString(); // for debugging

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        IID that = (IID) object;
        return Arrays.equals(this.bytes, that.bytes);
    }

    @Override
    public final int hashCode() {
        if (hash == 0) hash = Arrays.hashCode(bytes);
        return hash;
    }
}
