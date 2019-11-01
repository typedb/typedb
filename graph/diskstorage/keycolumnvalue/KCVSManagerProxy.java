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
