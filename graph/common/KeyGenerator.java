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

package com.vaticle.typedb.core.graph.common;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.iid.PrefixIID;
import com.vaticle.typedb.core.graph.iid.StructureIID;
import com.vaticle.typedb.core.graph.iid.VertexIID;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.vaticle.typedb.core.common.collection.ByteArray.encodeInt;
import static com.vaticle.typedb.core.common.collection.ByteArray.encodeLong;
import static com.vaticle.typedb.core.common.collection.ByteArray.encodeLongAsSorted;
import static com.vaticle.typedb.core.common.collection.ByteArray.encodeShortAsSorted;
import static com.vaticle.typedb.core.common.collection.ByteArray.join;
import static com.vaticle.typedb.core.common.collection.Bytes.INTEGER_SIZE;
import static com.vaticle.typedb.core.common.collection.Bytes.LONG_SIZE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.MAX_RULE_REACHED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.MAX_INSTANCE_REACHED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.MAX_SUBTYPE_REACHED;
import static com.vaticle.typedb.core.graph.common.Encoding.Key.BUFFERED;
import static com.vaticle.typedb.core.graph.common.Encoding.Key.PERSISTED;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Thing.ENTITY;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Thing.RELATION;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Thing.ROLE;
import static com.vaticle.typedb.core.graph.iid.VertexIID.Thing.DEFAULT_LENGTH;
import static com.vaticle.typedb.core.graph.iid.VertexIID.Thing.PREFIX_W_TYPE_LENGTH;

public class KeyGenerator {

    public abstract static class Schema {

        private static final long SHORT_MAX_VALUE = Short.MAX_VALUE - 64;
        private static final long SHORT_MIN_VALUE = Short.MIN_VALUE + 64;

        protected final AtomicInteger ruleKey;
        protected final ConcurrentMap<PrefixIID, AtomicInteger> typeKeys;
        protected final int initialValue;
        protected final int delta;

        Schema(int initialValue, int delta) {
            typeKeys = new ConcurrentHashMap<>();
            ruleKey = new AtomicInteger(initialValue);
            this.initialValue = initialValue;
            this.delta = delta;
        }

        public ByteArray forType(PrefixIID rootIID, Label rootLabel) {
            int key;
            if ((key = typeKeys.computeIfAbsent(rootIID, k -> new AtomicInteger(initialValue)).getAndAdd(delta)) >= SHORT_MAX_VALUE
                    || key <= SHORT_MIN_VALUE) {
                typeKeys.get(rootIID).addAndGet(-1 * delta);
                throw TypeDBException.of(MAX_SUBTYPE_REACHED, rootLabel, SHORT_MAX_VALUE);
            }
            return encodeShortAsSorted(key);
        }

        public ByteArray forRule() {
            int key;
            if ((key = ruleKey.getAndAdd(delta)) >= SHORT_MAX_VALUE || key <= SHORT_MIN_VALUE) {
                ruleKey.addAndGet(-1 * delta);
                throw TypeDBException.of(MAX_RULE_REACHED, SHORT_MAX_VALUE);
            }
            return encodeShortAsSorted(key);
        }

        public ByteArray serialise() {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            ByteArray typeKeysSize = encodeInt(typeKeys.size());
            bytes.write(typeKeysSize.getArray(), 0, typeKeysSize.length());
            for (Map.Entry<PrefixIID, AtomicInteger> typeKey : typeKeys.entrySet()) {
                ByteArray key = typeKey.getKey().bytes();
                bytes.write(key.getArray(), 0, key.length());
                ByteArray value = encodeInt(typeKey.getValue().get());
                bytes.write(value.getArray(), 0, value.length());
            }
            ByteArray ruleKeyValue = encodeInt(ruleKey.get());
            bytes.write(ruleKeyValue.getArray(), 0, ruleKeyValue.length());
            return ByteArray.of(bytes.toByteArray());
        }

        public void deserialise(ByteArray bytes) {
            int pos = 0;
            typeKeys.clear();
            int typeKeysSize = bytes.view(pos, pos + INTEGER_SIZE).decodeInt();
            pos += INTEGER_SIZE;
            for (int i = 0; i < typeKeysSize; i++) {
                PrefixIID key = PrefixIID.of(Encoding.Prefix.of(bytes.get(pos)));
                pos += 1;
                AtomicInteger value = new AtomicInteger(bytes.view(pos, pos + INTEGER_SIZE).decodeInt());
                pos += INTEGER_SIZE;
                typeKeys.put(key, value);
            }
            int ruleKeyValue = bytes.view(pos, pos + INTEGER_SIZE).decodeInt();
            pos += INTEGER_SIZE;
            ruleKey.set(ruleKeyValue);
            assert pos == bytes.length();
        }

        public static class Buffered extends Schema {

            public Buffered() {
                super(BUFFERED.initialValue(), BUFFERED.isIncrement() ? 1 : -1);
            }
        }

        public static class Persisted extends Schema {

            public Persisted() {
                super(PERSISTED.initialValue(), PERSISTED.isIncrement() ? 1 : -1);
            }

            public void sync(Storage.Schema storage) {
                syncTypeKeys(storage);
                syncRuleKey(storage);
            }

            private void syncTypeKeys(Storage.Schema storage) {
                for (Encoding.Vertex.Type encoding : Encoding.Vertex.Type.values()) {
                    ByteArray prefix = encoding.prefix().bytes();
                    ByteArray lastIID = storage.getLastKey(prefix);
                    AtomicInteger nextValue = lastIID != null ?
                            new AtomicInteger(lastIID.view(PrefixIID.LENGTH, VertexIID.Type.LENGTH).decodeSortedAsShort() + delta) :
                            new AtomicInteger(initialValue);
                    typeKeys.put(PrefixIID.of(encoding), nextValue);
                }
            }

            private void syncRuleKey(Storage.Schema storage) {
                ByteArray prefix = Encoding.Structure.RULE.prefix().bytes();
                ByteArray lastIID = storage.getLastKey(prefix);
                if (lastIID != null) {
                    ruleKey.set(lastIID.view(PrefixIID.LENGTH, StructureIID.Rule.LENGTH).decodeSortedAsShort() + delta);
                } else {
                    ruleKey.set(initialValue);
                }
            }
        }
    }

    public abstract static class Data {

        private static final long LONG_MAX_VALUE = Long.MAX_VALUE - 64;
        private static final long LONG_MIN_VALUE = Long.MIN_VALUE + 64;

        protected final ConcurrentMap<VertexIID.Type, AtomicLong> thingKeys;
        protected final int initialValue;
        protected final int delta;

        Data(int initialValue, int delta) {
            thingKeys = new ConcurrentHashMap<>();
            this.initialValue = initialValue;
            this.delta = delta;
        }

        public ByteArray forThing(VertexIID.Type typeIID, Label typeLabel) {
            long key;
            if ((key = thingKeys.computeIfAbsent(typeIID, k -> new AtomicLong(initialValue)).getAndAdd(delta)) >= LONG_MAX_VALUE
                    || key <= LONG_MIN_VALUE) {
                thingKeys.get(typeIID).addAndGet(-1 * delta);
                throw TypeDBException.of(MAX_INSTANCE_REACHED, typeLabel, LONG_MAX_VALUE);
            }
            return encodeLongAsSorted(key);
        }

        public ByteArray serialise() {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            ByteArray thingKeysSize = encodeInt(thingKeys.size());
            bytes.write(thingKeysSize.getArray(), 0, thingKeysSize.length());
            for (Map.Entry<VertexIID.Type, AtomicLong> thingKey : thingKeys.entrySet()) {
                ByteArray key = thingKey.getKey().bytes();
                bytes.write(key.getArray(), 0, key.length());
                ByteArray value = encodeLong(thingKey.getValue().get());
                bytes.write(value.getArray(), 0, value.length());
            }
            return ByteArray.of(bytes.toByteArray());
        }

        public void deserialise(ByteArray bytes) {
            int pos = 0;
            thingKeys.clear();
            int thingKeysSize = bytes.view(pos, pos + INTEGER_SIZE).decodeInt();
            pos += INTEGER_SIZE;
            for (int i = 0; i < thingKeysSize; i++) {
                VertexIID.Type key = VertexIID.Type.extract(bytes, pos);
                pos += key.bytes().length();
                AtomicLong value = new AtomicLong(bytes.view(pos, pos + LONG_SIZE).decodeLong());
                pos += LONG_SIZE;
                thingKeys.put(key, value);
            }
            assert pos == bytes.length();
        }

        public static class Buffered extends Data {

            public Buffered() {
                super(BUFFERED.initialValue(), BUFFERED.isIncrement() ? 1 : -1);
            }
        }

        public static class Persisted extends Data {

            public Persisted() {
                super(PERSISTED.initialValue(), PERSISTED.isIncrement() ? 1 : -1);
            }

            public void sync(Storage.Schema schemaStorage, Storage.Data dataStorage) {
                Encoding.Vertex.Thing[] thingsWithGeneratedIID = new Encoding.Vertex.Thing[]{ENTITY, RELATION, ROLE};

                for (Encoding.Vertex.Thing thingEncoding : thingsWithGeneratedIID) {
                    ByteArray typeEncoding = Encoding.Vertex.Type.of(thingEncoding).prefix().bytes();
                    FunctionalIterator<ByteArray> typeIterator = schemaStorage.iterate(typeEncoding, (iid, value) -> iid)
                            .filter(iid1 -> iid1.length() == VertexIID.Type.LENGTH);
                    while (typeIterator.hasNext()) {
                        ByteArray typeIID = typeIterator.next();
                        ByteArray prefix = join(thingEncoding.prefix().bytes(), typeIID);
                        ByteArray lastIID = dataStorage.getLastKey(prefix);
                        AtomicLong nextValue = lastIID != null ?
                                new AtomicLong(lastIID.view(PREFIX_W_TYPE_LENGTH, DEFAULT_LENGTH).decodeSortedAsLong() + delta) :
                                new AtomicLong(initialValue);
                        thingKeys.put(VertexIID.Type.of(typeIID), nextValue);
                    }
                }
            }
        }
    }
}
