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
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.Map;

/**
 * A KeyValueStoreManager where the stores maintain keys in their natural order.
 */
public interface OrderedKeyValueStoreManager extends KeyValueStoreManager {

    /**
     * Opens an ordered database by the given name. If the database does not exist, it is
     * created. If it has already been opened, the existing handle is returned.
     * <p>
     *
     * @param name Name of database
     * @return Database Handle
     */
    @Override
    OrderedKeyValueStore openDatabase(String name) throws BackendException;


    /**
     * Executes multiple mutations at once. Each store (identified by a string name) in the mutations map is associated
     * with a KVMutation that contains all the mutations for that particular store.
     * <p>
     * This is an optional operation. Check #getFeatures() if it is supported by a particular implementation.
     */
    void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException;

}
