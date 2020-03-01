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
import grakn.core.graph.diskstorage.BaseTransactionConfig;

import java.util.List;

/**
 * Generic interface to a backend storage engine.
 *
 */

public interface StoreManager {

    /**
     * Returns a transaction handle for a new transaction according to the given configuration.
     *
     * @return New Transaction Handle
     */
    StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException;

    /**
     * Closes the Storage Manager and all databases that have been opened.
     */
    void close() throws BackendException;


    /**
     * Deletes and clears all database in this storage manager.
     * <p>
     * ATTENTION: Invoking this method will delete ALL your data!!
     */
    void clearStorage() throws BackendException;

    /**
     * Check whether database exists in this storage manager.
     * @return Flag indicating whether database exists
     * @throws BackendException
     */
    boolean exists() throws BackendException;

    /**
     * Returns the features supported by this storage manager
     *
     * @return The supported features of this storage manager
     * see StoreFeatures
     */
    StoreFeatures getFeatures();

    /**
     * Return an identifier for the StoreManager. Two managers with the same
     * name would open databases that read and write the same underlying data;
     * two store managers with different names should be, for data read/write
     * purposes, completely isolated from each other.
     * <p>
     * Examples:
     * <ul>
     * <li>Cassandra keyspace</li>
     * <li>InMemoryStore heap address (i.e. default toString()).</li>
     * </ul>
     *
     * @return Name for this StoreManager
     */
    String getName();

    /**
     * Returns {@code KeyRange}s locally hosted on this machine. The start of
     * each {@code KeyRange} is inclusive. The end is exclusive. The start and
     * end must each be at least 4 bytes in length.
     *
     * @return A list of local key ranges
     * @throws UnsupportedOperationException
     *             if the underlying store does not support this operation.
     *             Check StoreFeatures#hasLocalKeyPartition() first.
     */
    List<KeyRange> getLocalKeyPartition() throws BackendException;

}
