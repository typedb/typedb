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

import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.StoreMetaData;
import grakn.core.graph.diskstorage.keycolumnvalue.KCVMutation;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.Map;

/**
 * KeyColumnValueStoreManager provides the persistence context to the graph database storage backend.
 * <p>
 * A KeyColumnValueStoreManager provides transaction handles across multiple data stores that
 * are managed by this KeyColumnValueStoreManager.
 *
 */
public interface KeyColumnValueStoreManager extends StoreManager {

    /**
     * Opens an ordered database by the given name. If the database does not exist, it is
     * created. If it has already been opened, the existing handle is returned.
     *
     * @param name Name of database
     * @return Database Handle
     *
     */
    default KeyColumnValueStore openDatabase(String name) throws BackendException {
        return openDatabase(name, StoreMetaData.EMPTY);
    }

    /**
     * Opens an ordered database by the given name. If the database does not exist, it is
     * created. If it has already been opened, the existing handle is returned.
     *
     * @param name Name of database
     * @param metaData options specific to this store
     * @return Database Handle
     *
     */
    KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException;

    /**
     * Executes multiple mutations at once. For each store (identified by a string name) there is a map of (key,mutation) pairs
     * that specifies all the mutations to execute against the particular store for that key.
     *
     * This is an optional operation. Check {@link #getFeatures()} if it is supported by a particular implementation.
     */
    void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException;

}
