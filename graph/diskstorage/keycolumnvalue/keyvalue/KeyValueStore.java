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
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;

/**
 * Interface for a data store that represents data in the simple key-&gt;value data model where each key is uniquely
 * associated with a value. Keys and values are generic ByteBuffers.
 */
public interface KeyValueStore {

    /**
     * Deletes the given key from the store.
     */
    void delete(StaticBuffer key, StoreTransaction txh) throws BackendException;

    /**
     * Returns the value associated with the given key.
     */
    StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException;

    /**
     * Returns true iff the store contains the given key, else false
     */
    boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException;


    /**
     * Acquires a lock for the given key and expected value (null, if not value is expected).
     */
    void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException;

    /**
     * Returns the name of this store
     */
    String getName();

    /**
     * Closes this store and releases its resources.
     */
    void close() throws BackendException;

}
