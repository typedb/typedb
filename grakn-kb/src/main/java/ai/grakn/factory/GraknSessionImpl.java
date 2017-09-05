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
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.kb.internal.GraknTxAbstract;
import ai.grakn.kb.internal.GraknTxTinker;
import ai.grakn.kb.internal.computer.GraknComputerImpl;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.REST;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static ai.grakn.util.EngineCommunicator.contactEngine;
import static ai.grakn.util.REST.Request.CONFIG_PARAM;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.WebPath.System.INITIALISE;
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
 * @author fppt
 */
public class GraknSessionImpl implements GraknSession {
    private final Logger LOG = LoggerFactory.getLogger(GraknSessionImpl.class);
    private final String location;
    private final String keyspace;

    //References so we don't have to open a tx just to check the count of the transactions
    private GraknTxAbstract<?> tx = null;
    private GraknTxAbstract<?> txBatch = null;

    //This is used to map grakn properties into the underlaying properties
    private static final Map<String, String> propertyMapper = new HashMap<>();
    static{
        //propertyMapper.put()
    }


    //This constructor must remain public because it is accessed via reflection
    public GraknSessionImpl(String keyspace, String location){
        this.location = location;
        this.keyspace = keyspace;
    }

    @Override
    public GraknTx open(GraknTxType transactionType) {
        final TxFactory<?> factory = getConfiguredFactory();
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

    private TxFactory<?> getConfiguredFactory(){
        return configureGraphFactory(keyspace, location, REST.KBConfig.DEFAULT);
    }

    /**
     * @return A new or existing grakn tx compute with the defined name
     */
    @Override
    public GraknComputer getGraphComputer() {
        TxFactory<?> configuredFactory = configureGraphFactory(keyspace, location, REST.KBConfig.COMPUTER);
        Graph graph = configuredFactory.getTinkerPopGraph(false);
        return new GraknComputerImpl(graph);
    }

    @Override
    public void close() throws GraknTxOperationException {
        int openTransactions = openTransactions(tx) + openTransactions(txBatch);
        if(openTransactions > 0){
            LOG.warn(ErrorMessage.TXS_OPEN.getMessage(this.keyspace, openTransactions));
        }

        //Close the main tx connections
        if(tx != null) tx.admin().closeSession();
        if(txBatch != null) txBatch.admin().closeSession();
    }

    private int openTransactions(GraknTxAbstract<?> graph){
        if(graph == null) return 0;
        return graph.numOpenTx();
    }

    /**
     * @param keyspace The keyspace of the tx
     * @param location The of where the tx is stored
     * @param graphType The type of tx to produce, default, batch, or compute
     * @return A new or existing grakn tx factory with the defined name connecting to the specified remote location
     */
    static TxFactory<?> configureGraphFactory(String keyspace, String location, String graphType){
        if(Grakn.IN_MEMORY.equals(location)){
            return configureGraphFactoryInMemory(keyspace);
        } else {
            return configureGraphFactoryRemote(keyspace, location, graphType);
        }
    }

    /**
     *
     * @param keyspace The keyspace of the tx
     * @param engineUrl The url of engine to get the tx factory config from
     * @param graphType The type of tx to produce, default, batch, or compute
     * @return A new or existing grakn tx factory with the defined name connecting to the specified remote location
     */
    private static TxFactory<?> configureGraphFactoryRemote(String keyspace, String engineUrl, String graphType){
        String restFactoryUri = engineUrl + INITIALISE + "?" + CONFIG_PARAM + "=" + graphType + "&" + KEYSPACE_PARAM + "=" + keyspace;

        Properties properties = new Properties();

        //Get Specific Configs
        properties.putAll(read(contactEngine(restFactoryUri, REST.HttpConn.GET_METHOD)).asMap());

        return FactoryBuilder.getFactory(keyspace, engineUrl, properties);
    }

    /**
     *
     * @param keyspace The keyspace of the tx
     * @return  A new or existing grakn tx factory with the defined name holding the tx in memory
     */
    private static TxFactory<?> configureGraphFactoryInMemory(String keyspace){
        Properties inMemoryProperties = new Properties();
        inMemoryProperties.put(GraknTxAbstract.SHARDING_THRESHOLD, 100_000);
        inMemoryProperties.put(GraknTxAbstract.NORMAL_CACHE_TIMEOUT_MS, 30_000);
        inMemoryProperties.put(FactoryBuilder.FACTORY_TYPE, TxFactoryTinker.class.getName());

        return FactoryBuilder.getFactory(TxFactoryTinker.class.getName(), keyspace, Grakn.IN_MEMORY, inMemoryProperties);
    }
}
