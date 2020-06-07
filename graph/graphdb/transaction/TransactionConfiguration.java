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

package grakn.core.graph.graphdb.transaction;

import grakn.core.graph.core.schema.DefaultSchemaMaker;
import grakn.core.graph.diskstorage.BaseTransactionConfig;

/**
 * Provides configuration options for JanusGraphTransaction.
 *
 * see JanusGraphTransaction
 */
public interface TransactionConfiguration extends BaseTransactionConfig {

    /**
     * Checks whether the graph transaction is configured as read-only.
     *
     * @return True, if the transaction is configured as read-only, else false.
     */
    boolean isReadOnly();

    /**
     * @return Whether this transaction is configured to assign idAuthorities immediately.
     */
    boolean hasAssignIDsImmediately();


    /**
     * Whether this transaction should be optimized for batch-loading, i.e. ingestion of lots of data.
     */
    boolean hasEnabledBatchLoading();

    /**
     * Whether the graph transaction is configured to verify that a vertex with the id GIVEN BY THE USER actually exists
     * in the database or not.
     * In other words, it is verified that user provided vertex ids (through public APIs) actually exist.
     *
     * @return True, if vertex existence is verified, else false
     */
    boolean hasVerifyExternalVertexExistence();

    /**
     * Whether the graph transaction is configured to verify that a vertex with the id actually exists
     * in the database or not on every retrieval.
     * In other words, it is always verified that a vertex for a given id exists, even if that id is retrieved internally
     * (through private APIs).
     * <p>
     * Hence, this is a defensive setting against data degradation, where edges and/or index entries might point to no
     * longer existing vertices. Use this setting with caution as it introduces additional overhead entailed by checking
     * the existence.
     * <p>
     * Unlike #hasVerifyExternalVertexExistence() this is about internally verifying ids.
     *
     * @return True, if vertex existence is verified, else false
     */
    boolean hasVerifyInternalVertexExistence();

    /**
     * @return The default edge type maker used to automatically create not yet existing edge types.
     */
    DefaultSchemaMaker getAutoSchemaMaker();

    /**
     * Whether this transaction loads all properties on a vertex when a single property is requested. This can be highly beneficial
     * when additional properties are requested on the same vertex at a later time. For vertices with very many properties
     * this might increase latencies of property fetching.
     *
     * @return True, if this transaction pre-fetches all properties
     */
    boolean hasPropertyPrefetching();

    /**
     * Whether this transaction is only accessed by a single thread.
     * If so, then certain data structures may be optimized for single threaded access since locking can be avoided.
     */
    boolean isSingleThreaded();

    /**
     * Whether this transaction is bound to a running thread.
     * If so, then elements in this transaction can expand their life cycle to the next transaction in the thread.
     */
    boolean isThreadBound();

    /**
     * The maximum number of recently-used vertices to cache in this transaction.
     * The recently-used vertex cache can include both clean and dirty vertices.
     */
    int getVertexCacheSize();

    /**
     * The initial size of the dirty (modified) vertex map used by a transaction.
     */
    int getDirtyVertexSize();

    /**
     * The maximum weight for the index cache store used in this particular transaction
     */
    long getIndexCacheWeight();

    /**
     * The name of the LOG to be used for logging the mutations in this transaction.
     * If the identifier is NULL the mutations will not be logged.
     */
    String getLogIdentifier();


    /**
     * Querying of partitioned vertices is restricted to the partitions returned by this
     * method. If the return value has length 0 all partitions are queried (i.e. unrestricted).
     */
    int[] getRestrictedPartitions();

    /**
     * Returns true if the queried partitions should be restricted in this transaction
     */
    boolean hasRestrictedPartitions();

}
