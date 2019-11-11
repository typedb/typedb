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

package grakn.core.graph.diskstorage.locking.consistentkey;


import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.BaseTransactionConfig;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.configuration.MergedConfiguration;
import grakn.core.graph.diskstorage.keycolumnvalue.KCVMutation;
import grakn.core.graph.diskstorage.keycolumnvalue.KCVSManagerProxy;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.locking.LockerProvider;
import grakn.core.graph.diskstorage.util.StandardBaseTransactionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;


public class ExpectedValueCheckingStoreManager extends KCVSManagerProxy {

    private final String lockStoreSuffix;
    private final LockerProvider lockerProvider;
    private final Duration maxReadTime;
    private final StoreFeatures storeFeatures;

    private final Map<String, ExpectedValueCheckingStore> stores;

    private static final Logger LOG = LoggerFactory.getLogger(ExpectedValueCheckingStoreManager.class);

    public ExpectedValueCheckingStoreManager(KeyColumnValueStoreManager storeManager, String lockStoreSuffix,
                                             LockerProvider lockerProvider, Duration maxReadTime) {
        super(storeManager);
        this.lockStoreSuffix = lockStoreSuffix;
        this.lockerProvider = lockerProvider;
        this.maxReadTime = maxReadTime;
        this.storeFeatures = new StandardStoreFeatures.Builder(storeManager.getFeatures()).locking(true).build();
        this.stores = new HashMap<>(6);
    }

    @Override
    public synchronized KeyColumnValueStore openDatabase(String name) throws BackendException {
        if (stores.containsKey(name)) return stores.get(name);
        KeyColumnValueStore store = manager.openDatabase(name);
        String lockerName = store.getName() + lockStoreSuffix;
        ExpectedValueCheckingStore wrappedStore = new ExpectedValueCheckingStore(store, lockerProvider.getLocker(lockerName));
        stores.put(name, wrappedStore);
        return wrappedStore;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        ExpectedValueCheckingTransaction etx = (ExpectedValueCheckingTransaction) txh;
        boolean hasAtLeastOneLock = etx.prepareForMutations();
        if (hasAtLeastOneLock) {
            // Force all mutations on this transaction to use strong consistency
            LOG.debug("Transaction {} holds one or more locks: writing using consistent transaction {} due to held locks", etx, etx.getConsistentTx());
            manager.mutateMany(mutations, etx.getConsistentTx());
        } else {
            LOG.debug("Transaction {} holds no locks: writing mutations using store transaction {}", etx, etx.getInconsistentTx());
            manager.mutateMany(mutations, etx.getInconsistentTx());
        }
    }

    @Override
    public ExpectedValueCheckingTransaction beginTransaction(BaseTransactionConfig configuration) throws BackendException {
        // Get a transaction without any guarantees about strong consistency
        StoreTransaction inconsistentTx = manager.beginTransaction(configuration);

        // Get a transaction that provides global strong consistency
        Configuration customOptions = new MergedConfiguration(storeFeatures.getKeyConsistentTxConfig(), configuration.getCustomOptions());
        BaseTransactionConfig consistentTxCfg = new StandardBaseTransactionConfig.Builder(configuration)
                .customOptions(customOptions).build();
        StoreTransaction strongConsistentTx = manager.beginTransaction(consistentTxCfg);

        // Return a wrapper around both the inconsistent and consistent store transactions
        return new ExpectedValueCheckingTransaction(inconsistentTx, strongConsistentTx, maxReadTime);
    }

    @Override
    public StoreFeatures getFeatures() {
        return storeFeatures;
    }

}
