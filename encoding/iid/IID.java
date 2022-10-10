/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.encoding.iid;

import com.vaticle.typedb.core.common.collection.ByteArray;

import java.util.Arrays;

public abstract class IID {

    String readableString; // for debugging
    final ByteArray bytes;

    IID(ByteArray bytes) {
        this.bytes = bytes;
    }

    public ByteArray bytes() {
        return bytes;
    }

    public boolean isEmpty() {
        return bytes.isEmpty();
    }

    @Override
    public abstract String toString(); // for debugging

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        IID that = (IID) object;
        return this.bytes.equals(that.bytes);
    }

    @Override
    public int hashCode() {
        return bytes.hashCode();
    }

    public static class Array {

        private final IID[] array;

        public Array(IID[] array) {
            this.array = array;
        }

        public IID get(int i) {
            return array[i];
        }

        public int length() {
            return array.length;
        }

        public IID[] array() {
            return array;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Array other = (Array) o;
            return Arrays.equals(array, other.array);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(array);
        }
    }
}
