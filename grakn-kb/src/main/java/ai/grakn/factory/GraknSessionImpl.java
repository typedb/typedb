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

package ai.grakn.factory;

import ai.grakn.Grakn;
import ai.grakn.GraknComputer;
import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.kb.internal.GraknTxAbstract;
import ai.grakn.kb.internal.GraknTxTinker;
import ai.grakn.kb.internal.computer.GraknComputerImpl;
import static ai.grakn.util.EngineCommunicator.contactEngine;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.REST;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.WebPath.System.INITIALISE;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import static mjson.Json.read;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * @author fppt
 */
public class GraknSessionImpl implements GraknSession {
    private static final Logger LOG = LoggerFactory.getLogger(GraknSessionImpl.class);
    private static final int LOG_SUBMISSION_PERIOD = 1;
    private final ScheduledExecutorService commitLogSubmitter;
    private final String engineUri;
    private final Keyspace keyspace;
    private final Properties properties;

    //References so we don't have to open a tx just to check the count of the transactions
    private GraknTxAbstract<?> tx = null;
    private GraknTxAbstract<?> txBatch = null;

    //This constructor must remain public because it is accessed via reflection
    public GraknSessionImpl(Keyspace keyspace, String engineUri){
        this.engineUri = engineUri;
        this.keyspace = keyspace;

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("commit-log-subbmit-%d").build();
        commitLogSubmitter = Executors.newSingleThreadScheduledExecutor(namedThreadFactory);
        commitLogSubmitter.scheduleAtFixedRate(() -> {
            submitLogs(tx);
            submitLogs(txBatch);
        }, 0, LOG_SUBMISSION_PERIOD, TimeUnit.SECONDS);

        if(Grakn.IN_MEMORY.equals(engineUri)){
            properties = getTxInMemoryProperties();
        } else {
            properties = getTxRemoteProperties(engineUri, keyspace);
        }
    }

    /**
     * Gets the properties needed to create a {@link GraknTx} by pinging engine for the config file
     *
     * @return the properties needed to build a {@link GraknTx}
     */
    private static Properties getTxRemoteProperties(String engineUrl, Keyspace keyspace){
        String restFactoryUri = engineUrl + INITIALISE + "?" + KEYSPACE_PARAM + "=" + keyspace;
        Properties properties = new Properties();
        //Get Specific Configs
        properties.putAll(read(contactEngine(restFactoryUri, REST.HttpConn.GET_METHOD)).asMap());
        return properties;
    }

    /**
     * Gets properties which let you build a toy in-mempoty {@link GraknTx}.
     * This does nto contact engine in anyway and can be run in an isolated manner
     *
     * @return the properties needed to build an in-memory {@link GraknTx}
     */
    private static Properties getTxInMemoryProperties(){
        Properties inMemoryProperties = new Properties();
        inMemoryProperties.put(GraknConfigKey.SHARDING_THRESHOLD, 100_000);
        inMemoryProperties.put(GraknTxAbstract.NORMAL_CACHE_TIMEOUT_MS, 30_000);
        inMemoryProperties.put(FactoryBuilder.KB_MODE, TxFactoryTinker.class.getName());
        return inMemoryProperties;
    }

    @Override
    public GraknTx open(GraknTxType transactionType) {
        final TxFactory<?> factory = configureTxFactory(REST.KBConfig.DEFAULT);
        switch (transactionType){
            case READ:
            case WRITE:
                tx = factory.open(transactionType);
                return tx;
            case BATCH:
                txBatch = factory.open(transactionType);
                return txBatch;
            default:
                throw GraknTxOperationException.transactionInvalid(transactionType);
        }
    }

    /**
     * @return A new or existing grakn tx compute with the defined name
     */
    @Override
    public GraknComputer getGraphComputer() {
        TxFactory<?> configuredFactory = configureTxFactory(REST.KBConfig.COMPUTER);
        Graph graph = configuredFactory.getTinkerPopGraph(false);
        return new GraknComputerImpl(graph);
    }

    @Override
    public void close() throws GraknTxOperationException {
        int openTransactions = openTransactions(tx) + openTransactions(txBatch);
        if(openTransactions > 0){
            LOG.warn(ErrorMessage.TXS_OPEN.getMessage(this.keyspace, openTransactions));
        }

        //Stop submitting commit logs automatically
        commitLogSubmitter.shutdown();

        //Close the main tx connections
        close(tx);
        close(txBatch);
    }

    private void close(GraknTxAbstract tx){
        if(tx != null){
            tx.closeSession();
            submitLogs(tx);
        }
    }

    private void submitLogs(GraknTxAbstract tx){
        if(tx != null) tx.commitLog().submit(engineUri, keyspace).ifPresent(LOG::debug);
    }

    private int openTransactions(GraknTxAbstract<?> graph){
        if(graph == null) return 0;
        return graph.numOpenTx();
    }

    /**
     * Gets a factory capable of building {@link GraknTx}s based on the provided config type.
     * The will either build an analytics factory or a normal {@link TxFactory}
     *
     * @param configType the type of factory to build, a normal {@link TxFactory} or an analytics one
     * @return the factory
     */
    TxFactory<?> configureTxFactory(String configType){
        if(Grakn.IN_MEMORY.equals(engineUri)){
            return FactoryBuilder.getFactory(TxFactoryTinker.class.getName(), keyspace, Grakn.IN_MEMORY, properties);
        } else {
            return configureTxFactoryRemote(configType);
        }
    }

    /**
     *
     * @param configType The type of {@link GraknTx} to produce, normal, or for analytics
     * @return A new or existing {@link TxFactory} with the defined name connecting to the specified remote location
     */
    private TxFactory<?> configureTxFactoryRemote(String configType){

        if(REST.KBConfig.DEFAULT.equals(configType)) {
            return FactoryBuilder.getFactory(keyspace, engineUri, properties);
        } else if(REST.KBConfig.COMPUTER.equals(configType)){
            Properties computerProperties = new Properties();
            computerProperties.putAll(properties);
            computerProperties.setProperty(FactoryBuilder.KB_MODE, properties.get(FactoryBuilder.KB_ANALYTICS).toString());
            return FactoryBuilder.getFactory(keyspace, engineUri, computerProperties);
        }

        throw new IllegalArgumentException("Config option [" + configType + "] not supported");
    }
}
