// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.diskstorage.keycolumnvalue.keyvalue;

import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.EntryMetaData;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.TemporaryBackendException;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import grakn.core.graph.diskstorage.keycolumnvalue.keyvalue.KeyValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import grakn.core.graph.diskstorage.util.RecordIterator;
import grakn.core.graph.diskstorage.util.StaticArrayEntry;
import grakn.core.graph.diskstorage.util.StaticArrayEntryList;

import java.io.IOException;

/**
 * Utility methods for interacting with {@link KeyValueStore}.
 */
public class KVUtil {

    public static EntryList getSlice(OrderedKeyValueStore store, StaticBuffer keyStart, StaticBuffer keyEnd, StoreTransaction txh) throws BackendException {
        return convert(store.getSlice(new KVQuery(keyStart,keyEnd), txh));
    }

    public static EntryList getSlice(OrderedKeyValueStore store, StaticBuffer keyStart, StaticBuffer keyEnd, int limit, StoreTransaction txh) throws BackendException {
        return convert(store.getSlice(new KVQuery(keyStart, keyEnd, limit), txh));
    }

    public static EntryList convert(RecordIterator<KeyValueEntry> iterator) throws BackendException {
        try {
            return StaticArrayEntryList.ofStaticBuffer(iterator, KVEntryGetter.INSTANCE);
        } finally {
            try {
                iterator.close();
            } catch (IOException e) {
                throw new TemporaryBackendException(e);
            }
        }
    }

    private enum KVEntryGetter implements StaticArrayEntry.GetColVal<KeyValueEntry, StaticBuffer> {
        INSTANCE;

        @Override
        public StaticBuffer getColumn(KeyValueEntry element) {
            return element.getKey();
        }

        @Override
        public StaticBuffer getValue(KeyValueEntry element) {
            return element.getValue();
        }

        @Override
        public EntryMetaData[] getMetaSchema(KeyValueEntry element) {
            return StaticArrayEntry.EMPTY_SCHEMA;
        }

        @Override
        public Object getMetaData(KeyValueEntry element, EntryMetaData meta) {
            throw new UnsupportedOperationException("Unsupported meta data: " + meta);
        }
    }

}
