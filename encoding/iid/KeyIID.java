/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.encoding.iid;

import com.vaticle.typedb.core.common.collection.ByteArray;

/**
 * Generated IID for a vertex without the type or other prefixes
 */
public class KeyIID extends IID {

    private KeyIID(ByteArray bytes) {
        super(bytes);
    }

    public static KeyIID of(ByteArray bytes) {
        return new KeyIID(bytes);
    }

    @Override
    public String toString() {
        if (readableString == null) readableString = "[" + bytes.length() + ": " + bytes.toString() + "]";
        return readableString;
    }
}
