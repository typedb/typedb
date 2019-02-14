/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.server.session;

import grakn.core.common.config.Config;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.keyspace.Keyspace;

import javax.annotation.CheckReturnValue;

/**
 * This class facilitates the construction of Transaction by determining which factory should be built.
 */
public class SessionImpl implements Session {

    private final TransactionOLTPFactory transactionOLTPFactory;
    private final TransactionOLAPFactory transactionOLAPFactory;

    private final Keyspace keyspace;
    private final Config config;

    //References so we don't have to open a tx just to check the count of the transactions
    private TransactionOLTP tx = null;

    /**
     * Instantiates {@link SessionImpl} specific for internal use (within Grakn Server),
     * using provided Grakn configuration
     *
     * @param keyspace to which keyspace the session should be bound to
     * @param config   config to be used. If null is supplied, it will be created
     */
    public SessionImpl(Keyspace keyspace, Config config) {
        this.keyspace = keyspace;
        this.config = config;
        this.transactionOLTPFactory = new TransactionOLTPFactory(this);
        this.transactionOLAPFactory = new TransactionOLAPFactory(this);
    }


    @Override
    public TransactionOLTP transaction(Transaction.Type type) {
        tx = transactionOLTPFactory.openOLTP(type);
        return tx;
    }

    /**
     * Get a new or existing TransactionOLAP.
     *
     * @return A new or existing Grakn graph computer
     * @see TransactionOLAP
     */
    @CheckReturnValue
    public TransactionOLAP transactionOLAP() {
        return transactionOLAPFactory.openOLAP();
    }

    @Override
    public void close() throws TransactionException {
        if (tx != null) {
            tx.closeSession();
            tx.closeOpenTransactions();
        }
    }

    @Override
    public Keyspace keyspace() {
        return keyspace;
    }

    /**
     * The config options of this {@link Session} which were passed in at the time of construction
     *
     * @return The config options of this {@link Session}
     */
    public Config config() {
        return config;
    }
}
