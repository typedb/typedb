/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn;


import ai.grakn.exception.GraphRuntimeException;

/**
 * <p>
 *     Builds a Grakn Graph factory
 * </p>
 *
 * <p>
 *     This class facilitates the construction of Grakn Graphs by determining which factory should be built.
 *     The graphs produced by a factory are singletons bound to a specific keyspace.
 *     To create graphs bound to a different keyspace you must create another factory
 *     using {@link Grakn#factory(String, String)}
 *
 * </p>
 *
 * @author fppt
 */
public interface GraknSession {
    String DEFAULT_URI = "localhost:4567";

    /**
     * Gets a new transaction bound to the keyspace of this Session.
     *
     * @param transactionType The type of transaction to open see {@link GraknTransaction} for more details
     * @return A new Grakn graph transaction
     * @see GraknGraph
     */
    GraknGraph open(GraknTransaction transactionType);

    /**
     * Get a new or existing graph with batch loading enabled.
     *
     * @return A new or existing Grakn graph with batch loading enabled
     * @see GraknGraph
     */
    GraknGraph getGraphBatchLoading();

    /**
     * Get a new or existing GraknComputer.
     *
     * @return A new or existing Grakn graph computer
     * @see GraknComputer
     */
    GraknComputer getGraphComputer();

    /**
     * Closes the main connection to the graph. This should be done at the end of using the graph.
     *
     * @throws GraphRuntimeException when more than 1 transaction is open on the graph
     */
    void close() throws GraphRuntimeException;

    /**
     *
     * @return The number of transactions open on the graph.
     */
    int openGraphTxs();

    /**
     *
     * @return The number of batch transactions open on the graph.
     */
    int openGraphBatchTxs();

}
