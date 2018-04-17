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

import ai.grakn.Grakn;
import ai.grakn.GraknComputer;
import ai.grakn.GraknConfigKey;
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
import ai.grakn.util.REST;
import ai.grakn.util.SimpleURI;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.CheckReturnValue;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static ai.grakn.util.EngineCommunicator.contactEngine;
import static mjson.Json.read;

/**
 * <p>
 *     Builds a {@link TxFactory}
 * </p>
 *
 * <p>
 *     This class facilitates the construction of {@link GraknTx} by determining which factory should be built.
 *     It does this by either defaulting to an in memory tx {@link GraknTxTinker} or by
 *     retrieving the factory definition from engine.
 *
 *     The deployment of engine decides on the backend and this class will handle producing the correct graphs.
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class EmbeddedGraknSession implements GraknSession {
    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedGraknSession.class);
    private static final int LOG_SUBMISSION_PERIOD = 1;
    private final String engineUri;
    private final Keyspace keyspace;
    private final GraknConfig config;
    private final boolean remoteSubmissionNeeded;
    private final CommitLogHandler commitLogHandler;
    private ScheduledExecutorService commitLogSubmitter;

    private final TxFactory<?> txFactory;
    private final TxFactory<?> computerTxFactory;



    //References so we don't have to open a tx just to check the count of the transactions
    private EmbeddedGraknTx<?> tx = null;
    private EmbeddedGraknTx<?> txBatch = null;

    /**
     * Instantiates {@link EmbeddedGraknSession}
     * @param keyspace to which keyspace the session should be bound to
     * @param engineUri to which Engine the session should be bound to
     * @param config config to be used. If null is supplied, it will be created
     * @param remoteSubmissionNeeded whether to create a background task which submits commit logs periodically
     */
    EmbeddedGraknSession(Keyspace keyspace, String engineUri, @Nullable GraknConfig config, boolean remoteSubmissionNeeded, TxFactoryBuilder txFactoryBuilder){
        Objects.requireNonNull(keyspace);
        Objects.requireNonNull(engineUri);

        this.remoteSubmissionNeeded = remoteSubmissionNeeded;
        this.engineUri = engineUri;
        this.keyspace = keyspace;

        //Create commit log submitter if needed
        if(remoteSubmissionNeeded) {
            ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                    .setNameFormat("commit-log-submit-%d").build();
            commitLogSubmitter = Executors.newSingleThreadScheduledExecutor(namedThreadFactory);
            commitLogSubmitter.scheduleAtFixedRate(this::submitLogs, 0, LOG_SUBMISSION_PERIOD, TimeUnit.SECONDS);
        }

        //Set properties directly or via a remote call
        if(config == null) {
            if (Grakn.IN_MEMORY.equals(engineUri)) {
                config = getTxInMemoryConfig();
            } else {
                config = getTxConfig();
            }
        }
        this.config = config;

        this.commitLogHandler = new CommitLogHandler(keyspace());


        this.txFactory = txFactoryBuilder.getFactory(this, false);
        this.computerTxFactory = txFactoryBuilder.getFactory(this, true);
    }

    public CommitLogHandler commitLogHandler(){
        return commitLogHandler;
    }

    /**
     * This methods creates a {@link EmbeddedGraknSession} object for the remote API.
     * A user should not call this method directly.
     * See {@link Grakn#session(String, String)} for creating a {@link GraknSession} for the remote API
     */
    //This must remain public because it is accessed via reflection from Grakn.session()
    // Also this method uses default TxFactoryBuilder implementation in Grakn core.
    @SuppressWarnings("unused")
    public static EmbeddedGraknSession create(Keyspace keyspace, String engineUri){
        return new EmbeddedGraknSession(keyspace, engineUri, null, true, GraknTxFactoryBuilder.getInstance());
    }

    /**
     * Creates a {@link EmbeddedGraknSession} specific for internal use (within Engine),
     * using provided Grakn configuration and disabling the remote (via REST) submission of commit log.
     */
    public static EmbeddedGraknSession createEngineSession(Keyspace keyspace, String engineUri, GraknConfig config, TxFactoryBuilder txFactoryBuilder){
        return new EmbeddedGraknSession(keyspace, engineUri, config, false, txFactoryBuilder);
    }

    GraknConfig getTxConfig(){
        SimpleURI uri = new SimpleURI(engineUri);
        return getTxRemoteConfig(uri, keyspace);
    }

    /**
     * Gets the properties needed to create a {@link GraknTx} by pinging engine for the config file
     *
     * @return the properties needed to build a {@link GraknTx}
     */
    private static GraknConfig getTxRemoteConfig(SimpleURI uri, Keyspace keyspace){
        URI keyspaceUri = UriBuilder.fromUri(uri.toURI()).path(REST.resolveTemplate(REST.WebPath.KB_KEYSPACE, keyspace.getValue())).build();

        Properties properties = new Properties();
        //Get Specific Configs
        properties.putAll(read(contactEngine(Optional.of(keyspaceUri), REST.HttpConn.PUT_METHOD)).asMap());

        GraknConfig config = GraknConfig.of(properties);

        //Overwrite Engine IP with something which is remotely accessible
        config.setConfigProperty(GraknConfigKey.SERVER_HOST_NAME, uri.getHost());
        config.setConfigProperty(GraknConfigKey.SERVER_PORT, uri.getPort());

        return config;
    }

    /**
     * Gets properties which let you build a toy in-mempoty {@link GraknTx}.
     * This does nto contact engine in anyway and can be run in an isolated manner
     *
     * @return the properties needed to build an in-memory {@link GraknTx}
     */
    static GraknConfig getTxInMemoryConfig(){
        GraknConfig config = GraknConfig.empty();
        config.setConfigProperty(GraknConfigKey.SHARDING_THRESHOLD, 100_000L);
        config.setConfigProperty(GraknConfigKey.SESSION_CACHE_TIMEOUT_MS, 30_000);
        config.setConfigProperty(GraknConfigKey.KB_MODE, GraknTxFactoryBuilder.IN_MEMORY);
        config.setConfigProperty(GraknConfigKey.KB_ANALYTICS, GraknTxFactoryBuilder.IN_MEMORY);
        return config;
    }

    @Override
    public EmbeddedGraknTx open(GraknTxType transactionType) {
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

        //Stop submitting commit logs automatically
        if(remoteSubmissionNeeded) commitLogSubmitter.shutdown();

        //Close the main tx connections
        submitLogs();
        if(tx != null) tx.closeSession();
        if(txBatch != null) txBatch.closeSession();
    }

    @Override
    public String uri() {
        return engineUri;
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

    protected void submitLogs(){
        commitLogHandler().submit(engineUri, keyspace).ifPresent(LOG::debug);
    }

    private int openTransactions(EmbeddedGraknTx<?> graph){
        if(graph == null) return 0;
        return graph.numOpenTx();
    }

}
