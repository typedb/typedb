/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.encoding.iid;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.encoding.Encoding;

public class PrefixIID extends IID {

    public static final int LENGTH = 1;

    private PrefixIID(ByteArray bytes) {
        super(bytes);
        assert bytes.length() == LENGTH;
    }

    public static PrefixIID of(Encoding.Prefix prefix) {
        return new PrefixIID(prefix.bytes());
    }

    public static PrefixIID of(Encoding.Vertex encoding) {
        return new PrefixIID(encoding.prefix().bytes());
    }

    @Override
    public String toString() {
        if (readableString == null) readableString = "[" + Encoding.Prefix.of(bytes.get(0)).toString() + "]";
        return readableString;
    }
}
