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
 *     Builds a Grakn Graph Session
 * </p>
 *
 * <p>
 *     This class facilitates the construction of Grakn Graphs by determining which session should be built.
 *     The graphs produced by a session are singletons bound to a specific keyspace.
 *     To create graphs bound to a different keyspace you must create another session
 *     using {@link Grakn#session(String, String)}
 *
 * </p>
 *
 * @author fppt
 */
public interface GraknSession extends AutoCloseable {
    String DEFAULT_URI = "localhost:4567";

    /**
     * Gets a new transaction bound to the keyspace of this Session.
     *
     * @param transactionType The type of transaction to open see {@link GraknTxType} for more details
     * @return A new Grakn graph transaction
     * @see GraknGraph
     */
    GraknGraph open(GraknTxType transactionType);

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
}
