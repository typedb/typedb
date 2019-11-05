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

package grakn.core.graph.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
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

import java.util.List;
import java.util.Map;


public class KCVSManagerProxy implements KeyColumnValueStoreManager {

    protected final KeyColumnValueStoreManager manager;

    public KCVSManagerProxy(KeyColumnValueStoreManager manager) {
        this.manager = Preconditions.checkNotNull(manager);
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
    public StoreFeatures getFeatures() {
        return manager.getFeatures();
    }

    @Override
    public String getName() {
        return manager.getName();
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        return manager.getLocalKeyPartition();
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        return manager.openDatabase(name, metaData);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        manager.mutateMany(mutations,txh);
    }

}
