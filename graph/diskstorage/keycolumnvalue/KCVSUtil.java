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

package grakn.core.graph.diskstorage.keycolumnvalue;

import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.util.BufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Contains static utility methods for operating on KeyColumnValueStore.
 */

public class KCVSUtil {

    private static final Logger LOG = LoggerFactory.getLogger(KeyColumnValueStore.class);

    /**
     * Retrieves the value for the specified column and key under the given transaction
     * from the store if such exists, otherwise returns NULL
     *
     * @param store  Store
     * @param key    Key
     * @param column Column
     * @param txh    Transaction
     * @return Value for key and column or NULL if such does not exist
     */
    public static StaticBuffer get(KeyColumnValueStore store, StaticBuffer key, StaticBuffer column, StoreTransaction txh) throws BackendException {
        KeySliceQuery query = new KeySliceQuery(key, column, BufferUtil.nextBiggerBuffer(column)).setLimit(2);
        List<Entry> result = store.getSlice(query, txh);
        if (result.size() > 1) {
            LOG.warn("GET query returned more than 1 result: store {} | key {} | column {}", store.getName(), key, column);
        }

        if (result.isEmpty()) return null;
        else return result.get(0).getValueAs(StaticBuffer.STATIC_FACTORY);
    }

    public static KeyIterator getKeys(KeyColumnValueStore store, StoreFeatures features, int keyLength, int sliceLength, StoreTransaction txh) throws BackendException {
        return getKeys(store, new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(sliceLength)).setLimit(1),
                features, keyLength, txh);
    }

    public static KeyIterator getKeys(KeyColumnValueStore store, SliceQuery slice, StoreFeatures features, int keyLength, StoreTransaction txh) throws BackendException {
        if (features.hasUnorderedScan()) {
            return store.getKeys(slice, txh);
        } else if (features.hasOrderedScan()) {
            return store.getKeys(new KeyRangeQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(keyLength), slice), txh);
        } else throw new UnsupportedOperationException("Provided stores does not support scan operations: " + store);
    }

    /**
     * Returns true if the specified key-column pair exists in the store.
     *
     * @param store  Store
     * @param key    Key
     * @param column Column
     * @param txh    Transaction
     * @return TRUE, if key has at least one column-value pair, else FALSE
     */
    public static boolean containsKeyColumn(KeyColumnValueStore store, StaticBuffer key, StaticBuffer column, StoreTransaction txh) throws BackendException {
        return get(store, key, column, txh) != null;
    }

    private static final StaticBuffer START = BufferUtil.zeroBuffer(1), END = BufferUtil.oneBuffer(32);

    public static boolean containsKey(KeyColumnValueStore store, StaticBuffer key, StoreTransaction txh) throws BackendException {
        return containsKey(store, key, 32, txh);
    }

    public static boolean containsKey(KeyColumnValueStore store, StaticBuffer key, int maxColumnLength, StoreTransaction txh) throws BackendException {
        final StaticBuffer end;
        if (maxColumnLength > 32) {
            end = BufferUtil.oneBuffer(maxColumnLength);
        } else {
            end = END;
        }
        return !store.getSlice(new KeySliceQuery(key, START, end).setLimit(1), txh).isEmpty();
    }

    public static boolean matches(SliceQuery query, StaticBuffer column) {
        return query.getSliceStart().compareTo(column) <= 0 && query.getSliceEnd().compareTo(column) > 0;
    }

    public static boolean matches(KeyRangeQuery query, StaticBuffer key, StaticBuffer column) {
        return matches(query, column) && query.getKeyStart().compareTo(key) <= 0 && query.getKeyEnd().compareTo(key) > 0;

    }
}
