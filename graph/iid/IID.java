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

package com.vaticle.typedb.core.graph.iid;

import com.vaticle.typedb.core.common.collection.ByteArray;

public abstract class IID implements Comparable<IID> {

    String readableString; // for debugging
    final ByteArray bytes;

    IID(ByteArray bytes) {
        this.bytes = ByteBuffer.wrap(bytes);
    }

    public ByteArray bytes() {
        return bytes.array();
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
    public final int hashCode() {
        return bytes.hashCode();
    };

    @Override
    public int compareTo(IID o) {
        return bytes.compareTo(o.bytes);
    }
}
