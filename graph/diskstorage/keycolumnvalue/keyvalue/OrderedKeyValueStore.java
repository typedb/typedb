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
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.util.RecordIterator;

import java.util.List;
import java.util.Map;

/**
 * A KeyValueStore where the keys are ordered such that keys can be retrieved in order.
 */
public interface OrderedKeyValueStore extends KeyValueStore {

    /**
     * Inserts the given key-value pair into the store. If the key already exists, its value is overwritten by the given one.
     */
    void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws BackendException;

    /**
     * Returns a list of all Key-value pairs (KeyValueEntry where the keys satisfy the given KVQuery.
     * That means, the key lies between the query's start and end buffers, satisfied the filter condition (if any) and the position
     * of the result in the result list iterator is less than the given limit.
     * <p>
     * The operation is executed inside the context of the given transaction.
     */
    RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException;


    /**
     * Like #getSlice(KVQuery, StoreTransaction) but executes
     * all of the given queries at once and returns a map of all the result sets of each query.
     * <p>
     * Only supported when the given store implementation supports multi-query, i.e.
     * StoreFeatures#hasMultiQuery() return true. Otherwise
     * this method may throw a UnsupportedOperationException.
     */
    Map<KVQuery, RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh) throws BackendException;

}
