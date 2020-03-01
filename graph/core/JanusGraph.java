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

import grakn.core.graph.core.schema.JanusGraphManagement;

public interface JanusGraph extends AutoCloseable{

    /* ---------------------------------------------------------------
     * Transactions and general admin
     * ---------------------------------------------------------------
     */

    /**
     * Opens a new thread-independent JanusGraphTransaction.
     * <p>
     * The transaction is open when it is returned but MUST be explicitly closed by calling JanusGraphTransaction#commit()
     * or JanusGraphTransaction#rollback() when it is no longer needed.
     * <p>
     * Note, that this returns a thread independent transaction object. It is not necessary to call this method
     * to use Blueprint's standard transaction framework which will automatically start a transaction with the first
     * operation on the graph.
     *
     * @return Transaction object representing a transactional context.
     */
    JanusGraphTransaction newTransaction();

    /**
     * Returns a TransactionBuilder to construct a new thread-independent JanusGraphTransaction.
     *
     * @return a new TransactionBuilder
     * see TransactionBuilder
     * see #newTransaction()
     */
    TransactionBuilder buildTransaction();

    /**
     * Returns the management system for this graph instance. The management system provides functionality
     * to change global configuration options, install indexes and inspect the graph schema.
     * <p>
     * The management system operates in its own transactional context which must be explicitly closed.
     */
    JanusGraphManagement openManagement();

    /**
     * Checks whether the graph is open.
     *
     * @return true, if the graph is open, else false.
     * see #close()
     */
    boolean isOpen();

    /**
     * Checks whether the graph is closed.
     *
     * @return true, if the graph has been closed, else false
     */
    boolean isClosed();

    /**
     * Closes the graph database.
     * <p>
     * Closing the graph database causes a disconnect and possible closing of the underlying storage backend
     * and a release of all occupied resources by this graph database.
     * Closing a graph database requires that all open thread-independent transactions have been closed -
     * otherwise they will be left abandoned.
     *
     * @throws JanusGraphException if closing the graph database caused errors in the storage backend
     */
    void close() throws JanusGraphException;
}
