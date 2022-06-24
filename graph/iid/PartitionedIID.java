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

package com.vaticle.typedb.core.graph.iid;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.graph.common.Storage;

import java.util.Objects;

public abstract class PartitionedIID extends IID implements Storage.Key {

    private int hash =  0;

    PartitionedIID(ByteArray bytes) {
        super(bytes);
    }

    @Override
    public final boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        PartitionedIID that = (PartitionedIID) object;
        return this.partition().equals(that.partition()) && this.bytes.equals(that.bytes);
    }

    @Override
    public final int hashCode() {
        if (hash == 0) hash = Objects.hash(bytes, partition());
        return hash;
    }
}
