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

import grakn.core.server.GraknComputer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.Keyspace;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.internal.TransactionImpl;
import grakn.core.server.kb.internal.computer.GraknComputerImpl;
import grakn.core.util.GraknConfig;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Builds a {@link TransactionFactory}. This class facilitates the construction of {@link Transaction} by determining which factory should be built.
 *
 */
public class SessionImpl implements Session {
    private static final Logger LOG = LoggerFactory.getLogger(SessionImpl.class);
    private final Keyspace keyspace;
    private final GraknConfig config;

    private final TransactionFactory<?> transactionOLTPFactory;
    private final TransactionFactory<?> transactionOLAPFactory;


    //References so we don't have to open a tx just to check the count of the transactions
    private TransactionImpl<?> tx = null;
    private TransactionImpl<?> txBatch = null;

    /**
     * Instantiates {@link SessionImpl}
     *
     * @param keyspace         to which keyspace the session should be bound to
     * @param config           config to be used. If null is supplied, it will be created
     * @param transactionFactoryBuilder
     */
    SessionImpl(Keyspace keyspace, @Nullable GraknConfig config, TransactionFactoryBuilder transactionFactoryBuilder) {
        Objects.requireNonNull(keyspace);

        this.keyspace = keyspace;
        this.config = config;
        this.transactionOLTPFactory = transactionFactoryBuilder.getFactory(this, false);
        this.transactionOLAPFactory = transactionFactoryBuilder.getFactory(this, true);
    }

    /**
     * Creates a {@link SessionImpl} specific for internal use (within Engine),
     * using provided Grakn configuration
     */
    public static SessionImpl createEngineSession(Keyspace keyspace, GraknConfig config, TransactionFactoryBuilder transactionFactoryBuilder) {
        return new SessionImpl(keyspace, config, transactionFactoryBuilder);
    }

    public static SessionImpl createEngineSession(Keyspace keyspace, GraknConfig config) {
        return new SessionImpl(keyspace, config, TransactionFactoryBuilder.getInstance());
    }

    public static SessionImpl createEngineSession(Keyspace keyspace) {
        return new SessionImpl(keyspace, GraknConfig.create(), TransactionFactoryBuilder.getInstance());
    }


    @Override
    public TransactionImpl transaction(Transaction.Type transactionType) {
        switch (transactionType) {
            case READ:
            case WRITE:
                tx = transactionOLTPFactory.open(transactionType);
                return tx;
            case BATCH:
                txBatch = transactionOLTPFactory.open(transactionType);
                return txBatch;
            default:
                throw TransactionException.transactionInvalid(transactionType);
        }
    }

    /**
     * Get a new or existing GraknComputer.
     *
     * @return A new or existing Grakn graph computer
     * @see GraknComputer
     */
    @CheckReturnValue
    public GraknComputer getGraphComputer() {
        Graph graph = transactionOLAPFactory.getTinkerPopGraph(false);
        return new GraknComputerImpl(graph);
    }

    @Override
    public void close() throws TransactionException {
        if (tx != null) {
            tx.closeSession();
            closeTransactions(tx);
        }

        if (txBatch != null){
            txBatch.closeSession();
            closeTransactions(txBatch);
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
    public GraknConfig config() {
        return config;
    }


    private void closeTransactions(TransactionImpl<?> tx) {
        ((TransactionOLTP) tx).closeOpenTransactions();
    }

}
