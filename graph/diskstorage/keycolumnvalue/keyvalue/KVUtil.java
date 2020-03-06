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

package grakn.core.graph.diskstorage.keycolumnvalue.keyvalue;

import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.EntryMetaData;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.TemporaryBackendException;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.util.RecordIterator;
import grakn.core.graph.diskstorage.util.StaticArrayEntry;
import grakn.core.graph.diskstorage.util.StaticArrayEntryList;

import java.io.IOException;

/**
 * Utility methods for interacting with KeyValueStore.
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
