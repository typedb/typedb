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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.BaseTransactionConfig;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.StoreMetaData;
import grakn.core.graph.diskstorage.keycolumnvalue.KCVMutation;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyRange;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Wraps a OrderedKeyValueStoreManager and exposes it as a KeyColumnValueStoreManager.
 * <p>
 * An optional mapping of key-length can be defined if it is known that the KeyColumnValueStore of a given
 * name has a static key length. See OrderedKeyValueStoreAdapter for more information.
 */
public class OrderedKeyValueStoreManagerAdapter implements KeyColumnValueStoreManager {

    private final OrderedKeyValueStoreManager manager;

    private final ImmutableMap<String, Integer> keyLengths;

    private final Map<String, OrderedKeyValueStoreAdapter> stores;

    public OrderedKeyValueStoreManagerAdapter(OrderedKeyValueStoreManager manager) {
        this(manager, new HashMap<>());
    }

    public OrderedKeyValueStoreManagerAdapter(OrderedKeyValueStoreManager manager, Map<String, Integer> keyLengths) {
        Preconditions.checkArgument(manager.getFeatures().isKeyOrdered(), "Expected backing store to be ordered: %s", manager);
        this.manager = manager;
        ImmutableMap.Builder<String, Integer> mb = ImmutableMap.builder();
        if (keyLengths != null && !keyLengths.isEmpty()) mb.putAll(keyLengths);
        this.keyLengths = mb.build();
        this.stores = new HashMap<>();
    }

    @Override
    public StoreFeatures getFeatures() {
        return manager.getFeatures();
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        return manager.beginTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        manager.close();
    }

    @Override
    public void clearStorage() throws BackendException {
        manager.clearStorage();
    }

    @Override
    public boolean exists() throws BackendException {
        return manager.exists();
    }

    @Override
    public synchronized OrderedKeyValueStoreAdapter openDatabase(String name) throws BackendException {
        return openDatabase(name, StoreMetaData.EMPTY);
    }

    @Override
    public synchronized OrderedKeyValueStoreAdapter openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        if (!stores.containsKey(name) || stores.get(name).isClosed()) {
            OrderedKeyValueStoreAdapter store = wrapKeyValueStore(manager.openDatabase(name), keyLengths);
            stores.put(name, store);
        }
        return stores.get(name);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        Map<String, KVMutation> converted = new HashMap<>(mutations.size());
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> storeEntry : mutations.entrySet()) {
            OrderedKeyValueStoreAdapter store = openDatabase(storeEntry.getKey());
            Preconditions.checkNotNull(store);

            KVMutation mut = new KVMutation();
            for (Map.Entry<StaticBuffer, KCVMutation> entry : storeEntry.getValue().entrySet()) {
                StaticBuffer key = entry.getKey();
                KCVMutation mutation = entry.getValue();
                if (mutation.hasAdditions()) {
                    for (Entry addition : mutation.getAdditions()) {
                        mut.addition(store.concatenate(key, addition));
                    }
                }

                if (mutation.hasDeletions()) {
                    for (StaticBuffer del : mutation.getDeletions()) {
                        mut.deletion(store.concatenate(key, del));
                    }
                }
            }
            converted.put(storeEntry.getKey(), mut);
        }
        manager.mutateMany(converted, txh);
    }

    private static OrderedKeyValueStoreAdapter wrapKeyValueStore(OrderedKeyValueStore store, Map<String, Integer> keyLengths) {
        String name = store.getName();
        if (keyLengths.containsKey(name)) {
            int keyLength = keyLengths.get(name);
            Preconditions.checkArgument(keyLength > 0);
            return new OrderedKeyValueStoreAdapter(store, keyLength);
        } else {
            return new OrderedKeyValueStoreAdapter(store);
        }
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        return manager.getLocalKeyPartition();
    }

    @Override
    public String getName() {
        return manager.getName();
    }
}
