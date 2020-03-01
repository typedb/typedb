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
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.StoreMetaData;

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
     * This is an optional operation. Check #getFeatures() if it is supported by a particular implementation.
     */
    void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException;

}
