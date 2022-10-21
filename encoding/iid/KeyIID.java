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
