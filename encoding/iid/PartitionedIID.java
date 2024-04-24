/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.encoding.iid;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.encoding.key.Key;

import java.util.Objects;

public abstract class PartitionedIID extends IID implements Key {

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
