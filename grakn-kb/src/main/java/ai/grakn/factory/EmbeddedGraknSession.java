/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.factory;

import ai.grakn.GraknComputer;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.kb.internal.GraknTxTinker;
import ai.grakn.kb.internal.computer.GraknComputerImpl;
import ai.grakn.kb.internal.log.CommitLogHandler;
import ai.grakn.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Builds a {@link TxFactory}. This class facilitates the construction of {@link GraknTx} by determining which factory should be built.
 * It does this by either defaulting to an in memory tx {@link GraknTxTinker} or by retrieving the factory definition from engine.
 * The deployment of engine decides on the backend and this class will handle producing the correct graphs.
 *
 * @author Grakn Warriors
 */
public class EmbeddedGraknSession implements GraknSession {
    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedGraknSession.class);
    private final Keyspace keyspace;
    private final GraknConfig config;
    private final CommitLogHandler commitLogHandler;

    private final TxFactory<?> txFactory;
    private final TxFactory<?> computerTxFactory;


    //References so we don't have to open a tx just to check the count of the transactions
    private EmbeddedGraknTx<?> tx = null;
    private EmbeddedGraknTx<?> txBatch = null;

    /**
     * Instantiates {@link EmbeddedGraknSession}
     * @param keyspace to which keyspace the session should be bound to
     * @param config config to be used. If null is supplied, it will be created
     */
    EmbeddedGraknSession(Keyspace keyspace, @Nullable GraknConfig config, TxFactoryBuilder txFactoryBuilder){
        Objects.requireNonNull(keyspace);

        this.keyspace = keyspace;
        this.config = config;
        this.commitLogHandler = new CommitLogHandler(keyspace());
        this.txFactory = txFactoryBuilder.getFactory(this, false);
        this.computerTxFactory = txFactoryBuilder.getFactory(this, true);
    }

    public CommitLogHandler commitLogHandler(){
        return commitLogHandler;
    }

    /**
     * Creates a {@link EmbeddedGraknSession} specific for internal use (within Engine),
     * using provided Grakn configuration
     */
    public static EmbeddedGraknSession createEngineSession(Keyspace keyspace, GraknConfig config, TxFactoryBuilder txFactoryBuilder){
        return new EmbeddedGraknSession(keyspace, config,  txFactoryBuilder);
    }


    @Override
    public EmbeddedGraknTx transaction(GraknTxType transactionType) {
        switch (transactionType){
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
        int openTransactions = openTransactions(tx) + openTransactions(txBatch);
        if(openTransactions > 0){
            LOG.warn(ErrorMessage.TXS_OPEN.getMessage(this.keyspace, openTransactions));
        }

        if(tx != null) tx.closeSession();
        if(txBatch != null) txBatch.closeSession();
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


    private int openTransactions(EmbeddedGraknTx<?> graph){
        if(graph == null) return 0;
        return graph.numOpenTx();
    }

}
