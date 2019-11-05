/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.diskstorage.keycolumnvalue.inmemory;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.BaseTransactionConfig;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.StoreMetaData;
import grakn.core.graph.diskstorage.common.AbstractStoreTransaction;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.keycolumnvalue.KCVMutation;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyRange;
import grakn.core.graph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.keycolumnvalue.inmemory.InMemoryKeyColumnValueStore;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory backend storage engine.
 *
 */

public class InMemoryStoreManager implements KeyColumnValueStoreManager {

    private final ConcurrentHashMap<String, InMemoryKeyColumnValueStore> stores;

    private final StoreFeatures features;

    public InMemoryStoreManager() {
        this(Configuration.EMPTY);
    }

    public InMemoryStoreManager(Configuration configuration) {

        stores = new ConcurrentHashMap<>();

        features = new StandardStoreFeatures.Builder()
            .orderedScan(true)
            .unorderedScan(true)
            .keyOrdered(true)
            .persists(false)
            .optimisticLocking(true)
            .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
            .build();
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new InMemoryTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        for (InMemoryKeyColumnValueStore store : stores.values()) {
            store.close();
        }
        stores.clear();
    }

    @Override
    public void clearStorage() throws BackendException {
        for (InMemoryKeyColumnValueStore store : stores.values()) {
            store.clear();
        }
        stores.clear();
    }

    @Override
    public boolean exists() throws BackendException {
        return !stores.isEmpty();
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        if (!stores.containsKey(name)) {
            stores.putIfAbsent(name, new InMemoryKeyColumnValueStore(name));
        }
        KeyColumnValueStore store = stores.get(name);
        Preconditions.checkNotNull(store);
        return store;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> storeMut : mutations.entrySet()) {
            KeyColumnValueStore store = stores.get(storeMut.getKey());
            Preconditions.checkNotNull(store);
            for (Map.Entry<StaticBuffer, KCVMutation> keyMut : storeMut.getValue().entrySet()) {
                store.mutate(keyMut.getKey(), keyMut.getValue().getAdditions(), keyMut.getValue().getDeletions(), txh);
            }
        }
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return toString();
    }

    private static class InMemoryTransaction extends AbstractStoreTransaction {

        public InMemoryTransaction(BaseTransactionConfig config) {
            super(config);
        }
    }
}
