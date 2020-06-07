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

import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.util.time.TimestampProviders;

/**
 * Describes features supported by a storage backend.
 */

public interface StoreFeatures {

    /**
     * Equivalent to calling #hasUnorderedScan() {@code ||}
     * #hasOrderedScan().
     */
    boolean hasScan();

    /**
     * Whether this storage backend supports global key scans via
     * KeyColumnValueStore#getKeys(SliceQuery, StoreTransaction).
     */
    boolean hasUnorderedScan();

    /**
     * Whether this storage backend supports global key scans via
     * KeyColumnValueStore#getKeys(KeyRangeQuery, StoreTransaction).
     */
    boolean hasOrderedScan();

    /**
     * Whether this storage backend supports query operations on multiple keys
     * via
     * KeyColumnValueStore#getSlice(java.util.List, SliceQuery, StoreTransaction)
     */
    boolean hasMultiQuery();

    /**
     * Whether this store supports locking via
     * KeyColumnValueStore#acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)
     */
    boolean hasLocking();

    /**
     * Whether this storage backend supports batch mutations via
     * KeyColumnValueStoreManager#mutateMany(java.util.Map, StoreTransaction).
     */
    boolean hasBatchMutation();

    /**
     * Whether this storage backend preserves key locality. This affects JanusGraph's
     * use of vertex ID partitioning.
     */
    boolean isKeyOrdered();

    /**
     * Whether this storage backend writes and reads data from more than one
     * machine.
     */
    boolean isDistributed();

    /**
     * Whether this storage backend's transactions support isolation.
     */
    boolean hasTxIsolation();

    /**
     * Whether this storage backend has a (possibly improper) subset of the
     * its accessible data stored locally, that is, partially available for
     * I/O operations without necessarily going over the network.
     * <p>
     * If this is true, then StoreManager#getLocalKeyPartition() must
     * return a valid list as described in that method.  If this is false, that
     * method will not be invoked.
     */
    boolean hasLocalKeyPartition();

    /**
     * Whether this storage backend provides strong consistency within each
     * key/row. This property is weaker than general strong consistency, since
     * reads and writes to different keys need not obey strong consistency.
     * "Key consistency" is shorthand for
     * "strong consistency at the key/row level".
     *
     * @return true if the backend supports key-level strong consistency
     */
    boolean isKeyConsistent();

    /**
     * Returns true if column-value entries in this storage backend are annotated with a timestamp,
     * else false. It is assumed that the timestamp matches the one set during the committing transaction.
     */
    boolean hasTimestamps();

    /**
     * If this storage backend supports one particular type of data
     * timestamp/version better than others. For example, HBase server-side TTLs
     * assume row timestamps are in milliseconds; some Cassandra client utils
     * assume timestamps in microseconds. This method should return null if the
     * backend has no preference for a specific timestamp resolution.
     * <p>
     * This method will be ignored by JanusGraph if #hasTimestamps() is
     * false.
     *
     * @return null or a Timestamps enum value
     */
    TimestampProviders getPreferredTimestamps();

    /**
     * Returns true if this storage backend support time-to-live (TTL) settings for column-value entries. If such a value
     * is provided as a meta-data annotation on the Entry, the entry will
     * disappear from the storage backend after the given amount of time. See references to
     * EntryMetaData#TTL for example usage in JanusGraph internals.
     * This is the finer-grained of the two TTL modes.
     *
     * @return true if the storage backend supports cell-level TTL, else false
     */
    boolean hasCellTTL();

    /**
     * Returns true if this storage backend supports time-to-live (TTL) settings on a per-store basis. That means, that
     * entries added to such a store will require after a configured amount of time.  Per-store TTL is represented
     * by StoreMetaData#TTL.  This is the coarser-grained of the two
     * TTL modes.
     *
     * @return true if the storage backend supports store-level TTL, else false
     */
    boolean hasStoreTTL();

    /**
     * Whether the backend supports data persistence. Return false if the backend is in-memory only.
     */
    boolean supportsPersistence();

    /**
     * Get a transaction configuration that enforces key consistency. This
     * method has undefined behavior when #isKeyConsistent() is
     * false.
     *
     * @return a key-consistent tx config
     */
    Configuration getKeyConsistentTxConfig();

    /**
     * Get a transaction configuration that enforces local key consistency.
     * "Local" has flexible meaning depending on the backend implementation. An
     * example is Cassandra's notion of LOCAL_QUORUM, which provides strong
     * consistency among all replicas in the same datacenter as the node
     * handling the request, but not nodes at other datacenters. This method has
     * undefined behavior when #isKeyConsistent() is false.
     * <p>
     * Backends which don't support the notion of "local" strong consistency may
     * return the same configuration returned by
     * #getKeyConsistentTxConfig().
     *
     * @return a locally (or globally) key-consistent tx config
     */
    Configuration getLocalKeyConsistentTxConfig();



    /**
     * Whether calls to this manager and its stores may be safely interrupted
     * without leaving the underlying system in an inconsistent state.
     */
    boolean supportsInterruption();

    /**
     * Whether the store will commit pending mutations optimistically and make other pending changes
     * to the same cells fail on tx.commit() (true) or will fail pending mutations pessimistically on tx.commit()
     * if other parallel transactions have already marked the relevant cells dirty.
     */
    boolean hasOptimisticLocking();

}
