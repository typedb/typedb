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
 */

package grakn.core.graph.diskstorage.util;

import com.google.common.hash.HashCode;
import grakn.core.graph.diskstorage.StaticBuffer;


public class HashingUtil {

    public enum HashLength {
        SHORT, LONG;

        public int length() {
            switch (this) {
                case SHORT: return 4;
                case LONG: return 8;
                default: throw new AssertionError("Unknown hash type: " + this);
            }
        }
    }

    private static final StaticBuffer.Factory<HashCode> SHORT_HASH_FACTORY = (array, offset, limit) -> HashUtility.SHORT.get().hashBytes(array, offset, limit);

    private static final StaticBuffer.Factory<HashCode> LONG_HASH_FACTORY = (array, offset, limit) -> HashUtility.LONG.get().hashBytes(array,offset,limit);

    public static StaticBuffer hashPrefixKey(HashLength hashPrefixLen, StaticBuffer key) {
        final int prefixLen = hashPrefixLen.length();
        final StaticBuffer.Factory<HashCode> hashFactory;
        switch (hashPrefixLen) {
            case SHORT:
                hashFactory = SHORT_HASH_FACTORY;
                break;
            case LONG:
                hashFactory = LONG_HASH_FACTORY;
                break;
            default: throw new IllegalArgumentException("Unknown hash prefix: " + hashPrefixLen);
        }

        HashCode hashcode = key.as(hashFactory);
        WriteByteBuffer newKey = new WriteByteBuffer(prefixLen+key.length());
        if (prefixLen==4) newKey.putInt(hashcode.asInt());
        else newKey.putLong(hashcode.asLong());
        newKey.putBytes(key);
        return newKey.getStaticBuffer();
    }

    public static StaticBuffer getKey(HashLength hashPrefixLen, StaticBuffer hasPrefixedKey) {
        return hasPrefixedKey.subrange(hashPrefixLen.length(), hasPrefixedKey.length() - hashPrefixLen.length());
    }

}
