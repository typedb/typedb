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

package hypergraph.graph.util;

import hypergraph.common.collection.ByteArray;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static hypergraph.common.collection.ByteArrays.join;
import static hypergraph.common.collection.ByteArrays.longToBytes;
import static hypergraph.common.collection.ByteArrays.shortToBytes;
import static java.nio.ByteBuffer.wrap;
import static java.util.Arrays.copyOfRange;

public abstract class KeyGenerator {

    protected final ConcurrentMap<ByteArray, AtomicInteger> typeKeys;
    protected final ConcurrentMap<ByteArray, AtomicLong> thingKeys;
    protected final int initialValue;
    protected final int delta;

    KeyGenerator(int initialValue, int delta) {
        typeKeys = new ConcurrentHashMap<>();
        thingKeys = new ConcurrentHashMap<>();
        this.initialValue = initialValue;
        this.delta = delta;
    }

    public byte[] forType(byte[] root) {
        return shortToBytes(typeKeys.computeIfAbsent(
                ByteArray.of(root), k -> new AtomicInteger(initialValue)
        ).getAndAdd(delta));
    }

    public byte[] forThing(byte[] type) {
        return longToBytes(thingKeys.computeIfAbsent(
                ByteArray.of(type), k -> new AtomicLong(initialValue)
        ).getAndAdd(delta));
    }

    public static class Buffered extends KeyGenerator {

        public Buffered() {
            super(Schema.Key.BUFFERED.initialValue(), Schema.Key.BUFFERED.isIncrement() ? 1 : -1);
        }
    }

    public static class Persisted extends KeyGenerator {

        public Persisted() {
            super(Schema.Key.PERSISTED.initialValue(), Schema.Key.PERSISTED.isIncrement() ? 1 : -1);
        }

        public void sync(Storage storage) {
            syncTypeKeys(storage);
            syncThingKeys(storage);
        }

        private void syncTypeKeys(Storage storage) {
            for (Schema.Vertex.Type schema : Schema.Vertex.Type.values()) {
                byte[] prefix = schema.prefix().key();
                byte[] lastIID = storage.getLastKey(prefix);
                AtomicInteger nextValue = lastIID != null ?
                        new AtomicInteger(wrap(copyOfRange(lastIID, prefix.length, lastIID.length)).getShort() + delta) :
                        new AtomicInteger(initialValue);
                typeKeys.put(ByteArray.of(schema.prefix().key()), nextValue);
            }
        }

        private void syncThingKeys(Storage storage) {
            for (Schema.Vertex.Thing thingSchema : Schema.Vertex.Thing.values()) {
                byte[] typeIID = Schema.Vertex.Type.of(thingSchema).prefix().key();
                Iterator<byte[]> typeIterator = storage.iterate(typeIID, (iid, value) -> iid);
                while (typeIterator.hasNext()) {
                    byte[] prefix = join(thingSchema.prefix().key(), typeIterator.next());
                    byte[] lastIID = storage.getLastKey(prefix);
                    AtomicLong nextValue = lastIID != null ?
                            new AtomicLong(wrap(copyOfRange(lastIID, prefix.length, lastIID.length)).getShort() + delta) :
                            new AtomicLong(initialValue);
                    thingKeys.put(ByteArray.of(typeIID), nextValue);
                }
            }
        }
    }
}
