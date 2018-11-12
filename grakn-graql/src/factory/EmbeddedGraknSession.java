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

package grakn.core.factory;

import grakn.core.GraknComputer;
import grakn.core.GraknSession;
import grakn.core.GraknTx;
import grakn.core.GraknTxType;
import grakn.core.Keyspace;
import grakn.core.exception.GraknTxOperationException;
import grakn.core.janus.GraknTxJanus;
import grakn.core.kb.internal.EmbeddedGraknTx;
import grakn.core.kb.internal.computer.GraknComputerImpl;
import grakn.core.util.GraknConfig;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Builds a {@link TxFactory}. This class facilitates the construction of {@link GraknTx} by determining which factory should be built.
 *
 */
public class EmbeddedGraknSession implements GraknSession {
    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedGraknSession.class);
    private final Keyspace keyspace;
    private final GraknConfig config;

    private final TxFactory<?> txFactory;
    private final TxFactory<?> computerTxFactory;


    //References so we don't have to open a tx just to check the count of the transactions
    private EmbeddedGraknTx<?> tx = null;
    private EmbeddedGraknTx<?> txBatch = null;

    /**
     * Instantiates {@link EmbeddedGraknSession}
     *
     * @param keyspace         to which keyspace the session should be bound to
     * @param config           config to be used. If null is supplied, it will be created
     * @param txFactoryBuilder
     */
    EmbeddedGraknSession(Keyspace keyspace, @Nullable GraknConfig config, TxFactoryBuilder txFactoryBuilder) {
        Objects.requireNonNull(keyspace);

        this.keyspace = keyspace;
        this.config = config;
        this.txFactory = txFactoryBuilder.getFactory(this, false);
        this.computerTxFactory = txFactoryBuilder.getFactory(this, true);
    }

    /**
     * Creates a {@link EmbeddedGraknSession} specific for internal use (within Engine),
     * using provided Grakn configuration
     */
    public static EmbeddedGraknSession createEngineSession(Keyspace keyspace, GraknConfig config, TxFactoryBuilder txFactoryBuilder) {
        return new EmbeddedGraknSession(keyspace, config, txFactoryBuilder);
    }

    public static EmbeddedGraknSession createEngineSession(Keyspace keyspace, GraknConfig config) {
        return new EmbeddedGraknSession(keyspace, config, TxFactoryBuilder.getInstance());
    }

    public static EmbeddedGraknSession createEngineSession(Keyspace keyspace) {
        return new EmbeddedGraknSession(keyspace, GraknConfig.create(), TxFactoryBuilder.getInstance());
    }


    @Override
    public EmbeddedGraknTx transaction(GraknTxType transactionType) {
        switch (transactionType) {
            case READ:
            case WRITE:
                tx = txFactory.open(transactionType);
                return tx;
            case BATCH:
                txBatch = txFactory.open(transactionType);
                return txBatch;
            default:
                throw GraknTxOperationException.transactionInvalid(transactionType);
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
        Graph graph = computerTxFactory.getTinkerPopGraph(false);
        return new GraknComputerImpl(graph);
    }

    @Override
    public void close() throws GraknTxOperationException {
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
     * The config options of this {@link GraknSession} which were passed in at the time of construction
     *
     * @return The config options of this {@link GraknSession}
     */
    public GraknConfig config() {
        return config;
    }


    private void closeTransactions(EmbeddedGraknTx<?> tx) {
        ((GraknTxJanus) tx).closeOpenTransactions();
    }

}
