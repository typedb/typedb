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

package grakn.core.graph.diskstorage.keycolumnvalue.ttl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.BaseTransactionConfig;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.EntryMetaData;
import grakn.core.graph.diskstorage.MetaAnnotatable;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.StoreMetaData;
import grakn.core.graph.diskstorage.keycolumnvalue.KCVMutation;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyRange;
import grakn.core.graph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Turns a store with fine-grained cell-level TTL support into a store
 * with coarse-grained store-level (column-family-level) TTL support.
 * Useful when running a KCVSLog atop Cassandra.  Cassandra has
 * cell-level TTL support, but KCVSLog just wants to write all of its
 * data with a fixed, CF-wide TTL.  This class stores a fixed TTL set
 * during construction and applies it to every entry written through
 * subsequent mutate/mutateMany calls.
 */
public class TTLKCVSManager implements KeyColumnValueStoreManager {

    private final KeyColumnValueStoreManager manager;
    private final StoreFeatures features;
    private final Map<String, Integer> ttlEnabledStores = Maps.newConcurrentMap();

    public TTLKCVSManager(KeyColumnValueStoreManager manager) {
        this.manager = manager;
        Preconditions.checkArgument(manager.getFeatures().hasCellTTL());
        Preconditions.checkArgument(!manager.getFeatures().hasStoreTTL(),
                "Using TTLKCVSManager with %s is redundant: underlying implementation already supports store-level ttl",
                manager);
        this.features = new StandardStoreFeatures.Builder(manager.getFeatures()).storeTTL(true).build();
    }

    /**
     * Returns true if the parameter supports at least one of the following:
     *
     * <ul>
     * <li>cell-level TTL StoreFeatures#hasCellTTL()</li>
     * <li>store-level TTL StoreFeatures#hasStoreTTL()</li>
     * </ul>
     *
     * @param features an arbitrary {@code StoreFeatures} instance
     * @return true if and only if at least one TTL mode is supported
     */
    public static boolean supportsAnyTTL(StoreFeatures features) {
        return features.hasCellTTL() || features.hasStoreTTL();
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        KeyColumnValueStore store = manager.openDatabase(name);
        int storeTTL = metaData.contains(StoreMetaData.TTL) ? metaData.get(StoreMetaData.TTL) : -1;
        Preconditions.checkArgument(storeTTL > 0, "TTL must be positive: %s", storeTTL);
        ttlEnabledStores.put(name, storeTTL);
        return new TTLKCVS(store, storeTTL);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        if (!manager.getFeatures().hasStoreTTL()) {
            for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> sentry : mutations.entrySet()) {
                Integer ttl = ttlEnabledStores.get(sentry.getKey());
                if (null != ttl && 0 < ttl) {
                    for (KCVMutation mut : sentry.getValue().values()) {
                        if (mut.hasAdditions()) applyTTL(mut.getAdditions(), ttl);
                    }
                }
            }
        }
        manager.mutateMany(mutations, txh);
    }

    public static void applyTTL(Collection<Entry> additions, int ttl) {
        for (Entry entry : additions) {
            ((MetaAnnotatable) entry).setMetaData(EntryMetaData.TTL, ttl);
        }
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
    public String getName() {
        return manager.getName();
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        return manager.getLocalKeyPartition();
    }


}
