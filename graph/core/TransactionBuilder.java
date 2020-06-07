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

package grakn.core.graph.core;

import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;

import java.time.Instant;

/**
 * Constructor returned by JanusGraph#buildTransaction() to build a new transaction.
 * The TransactionBuilder allows certain aspects of the resulting transaction to be configured up-front.
 */
public interface TransactionBuilder {

    /**
     * Makes the transaction read only. Any writes will cause an exception.
     * Read-only transactions do not have to maintain certain data structures and can hence be more efficient.
     *
     * @return Object containing read-only properties set to true
     */
    TransactionBuilder readOnly();

    /**
     * Enabling batch loading disables a number of consistency checks inside JanusGraph to speed up the ingestion of
     * data under the assumptions that inconsistencies are resolved prior to loading.
     *
     * @return Object containting properties that will enable batch loading
     */
    TransactionBuilder enableBatchLoading();

    /**
     * Disables batch loading by ensuring that consistency checks are applied in this transaction. This allows
     * an individual transaction to use consistency checks when the graph as a whole is configured to not use them,
     * which is useful when defining schema elements in a graph with batch-loading enabled.
     *
     * @return Object containting properties that will disable batch loading
     */
    TransactionBuilder disableBatchLoading();

    /**
     * Configures the size of the internal caches used in the transaction.
     *
     * @param size The size of the initial cache for the transaction
     * @return Object containing the internal cache properties
     */
    TransactionBuilder vertexCacheSize(int size);

    /**
     * Configures the initial size of the map of modified vertices held by this
     * transaction. This is a performance hint, not a hard upper bound. The map
     * will grow if the transaction ends up modifying more vertices than
     * expected.
     *
     * @param size The initial size of the transaction's dirty vertex collection
     * @return Object containing properties that configure inital map size of modified vertices
     */
    TransactionBuilder dirtyVertexSize(int size);

    /**
     * Enables/disables checks that verify that each vertex actually exists in the underlying data store when it is retrieved.
     * This might be useful to address common data degradation issues but has adverse impacts on performance due to
     * repeated existence checks.
     * <p>
     * Note, that these checks apply to vertex retrievals inside the query execution engine and not to vertex ids provided
     * by the user.
     *
     * @param enabled Enable or disable the internal vertex existence checks
     * @return Object with the internal vertex existence check properties
     */
    TransactionBuilder checkInternalVertexExistence(boolean enabled);

    /**
     * Enables/disables checking whether the vertex with a user provided id indeed exists. If the user is absolutely sure
     * that the vertices for the ids provided in this transaction exist in the underlying data store, then disabling the
     * vertex existence check will improve performance because it eliminates a database call.
     * However, if a provided vertex id does not exist in the database and checking is disabled, JanusGraph will assume it
     * exists which can lead to data and query inconsistencies.
     *
     * @param enabled Enable or disable the external vertex existence checks
     * @return Object with the external vertex existence check properties
     */
    TransactionBuilder checkExternalVertexExistence(boolean enabled);

    /**
     * Sets the timestamp for this transaction. The transaction will be recorded
     * with this timestamp in those storage backends where the timestamp is
     * recorded.
     *
     * @param instant The instant at which the commit took place
     * @return Object with the commit time property
     */
    TransactionBuilder commitTime(Instant instant);

    /**
     * Name of the LOG to be used for logging the mutations in this transaction. If no LOG identifier is set,
     * then this transaction will not be logged.
     *
     * @param logName name of transaction LOG
     * @return Object containing LOG identifier property
     */
    TransactionBuilder logIdentifier(String logName);

    /**
     * Configures this transaction such that queries against partitioned vertices are
     * restricted to the given partitions.
     *
     * @param partitions Array of the int identifier of the partitions to be queried
     * @return Object with restricted partitions
     */
    TransactionBuilder restrictedPartitions(int[] partitions);


    /**
     * Starts and returns the transaction build by this builder
     *
     * @return A new transaction configured according to this builder
     */
    StandardJanusGraphTx start();

}
