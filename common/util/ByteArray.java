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

import java.util.Arrays;

public class ByteArray implements Comparable<ByteArray> {

    private byte[] array;

    private ByteArray(byte[] array) {
        this.array = array;
    }

    byte[] getBytes() {
        return array;
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
}
