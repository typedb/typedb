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

package grakn.core.graph.diskstorage.util;

import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.BaseTransactionConfig;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.StoreMetaData;
import grakn.core.graph.diskstorage.keycolumnvalue.KCVMutation;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyRange;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.util.stats.MetricManager;

import java.util.List;
import java.util.Map;


public class MetricInstrumentedStoreManager implements KeyColumnValueStoreManager {

    public static final String M_OPEN_DATABASE = "openDatabase";
    public static final String M_START_TX = "startTransaction";
    public static final String M_CLOSE_MANAGER = "closeManager";


    public static final String GLOBAL_PREFIX = "global";

    private final KeyColumnValueStoreManager backend;
    private final boolean mergeStoreMetrics;
    private final String mergedMetricsName;
    private final String managerMetricsName;

    public MetricInstrumentedStoreManager(KeyColumnValueStoreManager backend, String managerMetricsName,
                                          boolean mergeStoreMetrics, String mergedMetricsName) {
        this.backend = backend;
        this.mergeStoreMetrics = mergeStoreMetrics;
        this.mergedMetricsName = mergedMetricsName;
        this.managerMetricsName = managerMetricsName;
    }


    private String getMetricsStoreName(String storeName) {
        return mergeStoreMetrics ? mergedMetricsName : storeName;
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        //TODO-reenable
//        MetricManager.INSTANCE.getCounter(GLOBAL_PREFIX, managerMetricsName, M_OPEN_DATABASE, M_CALLS).inc();
        return new MetricInstrumentedStore(backend.openDatabase(name, metaData), getMetricsStoreName(name));
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        if (!txh.getConfiguration().hasGroupName()) {
            backend.mutateMany(mutations, txh);
        }
        String prefix = txh.getConfiguration().getGroupName();

        final MetricManager mgr = MetricManager.INSTANCE;
        //todo-reenable
//        mgr.getCounter(prefix, managerMetricsName, M_MUTATE, M_CALLS).inc();
//        final Timer.Context tc = mgr.getTimer(prefix,  managerMetricsName, M_MUTATE, M_TIME).time();

//        try {
//            backend.mutateMany(mutations,txh);
//        } catch (BackendException | RuntimeException e) {
//            mgr.getCounter(prefix,  managerMetricsName, M_MUTATE, M_EXCEPTIONS).inc();
//            throw e;
//        } finally {
//            tc.stop();
//        }
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
//        MetricManager.INSTANCE.getCounter(GLOBAL_PREFIX, managerMetricsName, M_START_TX, M_CALLS).inc();
        return backend.beginTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        backend.close();
//        MetricManager.INSTANCE.getCounter(GLOBAL_PREFIX, managerMetricsName, M_CLOSE_MANAGER, M_CALLS).inc();
    }

    @Override
    public void clearStorage() throws BackendException {
        backend.clearStorage();
    }

    @Override
    public boolean exists() throws BackendException {
        return backend.exists();
    }

    @Override
    public StoreFeatures getFeatures() {
        return backend.getFeatures();
    }

    @Override
    public String getName() {
        return backend.getName();
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        return backend.getLocalKeyPartition();
    }
}
