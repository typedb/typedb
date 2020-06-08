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

import hypergraph.graph.iid.IID;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static hypergraph.common.collection.Bytes.join;
import static hypergraph.common.collection.Bytes.longToBytes;
import static hypergraph.common.collection.Bytes.shortToBytes;
import static hypergraph.common.iterator.Iterators.filter;
import static java.nio.ByteBuffer.wrap;
import static java.util.Arrays.copyOfRange;

public abstract class KeyGenerator {

    protected final ConcurrentMap<IID.Prefix, AtomicInteger> typeKeys;
    protected final ConcurrentMap<IID.Vertex.Type, AtomicLong> thingKeys;
    protected final int initialValue;
    protected final int delta;

    KeyGenerator(int initialValue, int delta) {
        typeKeys = new ConcurrentHashMap<>();
        thingKeys = new ConcurrentHashMap<>();
        this.initialValue = initialValue;
        this.delta = delta;
    }

    public byte[] forType(IID.Prefix root) {
        return shortToBytes(typeKeys.computeIfAbsent(
                root, k -> new AtomicInteger(initialValue)
        ).getAndAdd(delta));
    }

    public byte[] forThing(IID.Vertex.Type typeIID) {
        return longToBytes(thingKeys.computeIfAbsent(
                typeIID, k -> new AtomicLong(initialValue)
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
                byte[] prefix = schema.prefix().bytes();
                byte[] lastIID = storage.getLastKey(prefix);
                AtomicInteger nextValue = lastIID != null ?
                        new AtomicInteger(wrap(copyOfRange(lastIID, IID.Prefix.LENGTH, IID.Vertex.Type.LENGTH)).getShort() + delta) :
                        new AtomicInteger(initialValue);
                typeKeys.put(IID.Prefix.of(schema.prefix().bytes()), nextValue);
            }
        }

        private void syncThingKeys(Storage storage) {
            Schema.Vertex.Thing[] thingsWithGeneratedIID = new Schema.Vertex.Thing[]{
                    Schema.Vertex.Thing.ENTITY, Schema.Vertex.Thing.RELATION, Schema.Vertex.Thing.ROLE
            };

            for (Schema.Vertex.Thing thingSchema : thingsWithGeneratedIID) {
                byte[] typeSchema = thingSchema.type().prefix().bytes();
                Iterator<byte[]> typeIterator = filter(storage.iterate(typeSchema, (iid, value) -> iid),
                                                       iid -> iid.length == IID.Vertex.Type.LENGTH);
                while (typeIterator.hasNext()) {
                    byte[] typeIID = typeIterator.next();
                    byte[] prefix = join(thingSchema.prefix().bytes(), typeIID);
                    byte[] lastIID = storage.getLastKey(prefix);
                    AtomicLong nextValue = lastIID != null ?
                            new AtomicLong(wrap(
                                    copyOfRange(lastIID, IID.Vertex.Thing.PREFIX_TYPE_LENGTH, IID.Vertex.Thing.LENGTH)
                            ).getShort() + delta) :
                            new AtomicLong(initialValue);
                    thingKeys.put(IID.Vertex.Type.of(typeIID), nextValue);
                }
            }
        }
    }
}
