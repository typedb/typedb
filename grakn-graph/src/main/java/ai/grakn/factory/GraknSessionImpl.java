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
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graph.internal.computer.GraknComputerImpl;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.REST;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static ai.grakn.util.EngineCommunicator.contactEngine;
import static ai.grakn.util.REST.Request.GRAPH_CONFIG_PARAM;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.WebPath.System.INITIALISE;
import static mjson.Json.read;

/**
 * <p>
 *     Builds a Grakn Graph factory
 * </p>
 *
 * <p>
 *     This class facilitates the construction of Grakn Graphs by determining which factory should be built.
 *     It does this by either defaulting to an in memory graph {@link ai.grakn.graph.internal.GraknTinkerGraph} or by
 *     retrieving the factory definition from engine.
 *
 *     The deployer of engine decides on the backend and this class will handle producing the correct graphs.
 * </p>
 *
 * @author fppt
 */
public class GraknSessionImpl implements GraknSession {
    private final Logger LOG = LoggerFactory.getLogger(GraknSessionImpl.class);
    private final String location;
    private final String keyspace;

    //References so we don't have to open a graph just to check the count of the transactions
    private AbstractGraknGraph<?> graph = null;
    private AbstractGraknGraph<?> graphBatch = null;

    //This constructor must remain public because it is accessed via reflection
    public GraknSessionImpl(String keyspace, String location){
        this.location = location;
        this.keyspace = keyspace;
    }

    @Override
    public GraknGraph open(GraknTxType transactionType) {
        final InternalFactory<?> factory = getConfiguredFactory();
        switch (transactionType){
            case READ:
            case WRITE:
                graph = factory.open(transactionType);
                return graph;
            case BATCH:
                graphBatch = factory.open(transactionType);
                return graphBatch;
            default:
                throw GraphOperationException.transactionInvalid(transactionType);
        }
    }

    private InternalFactory<?> getConfiguredFactory(){
        return configureGraphFactory(keyspace, location, REST.GraphConfig.DEFAULT);
    }

    /**
     * @return A new or existing grakn graph compute with the defined name
     */
    @Override
    public GraknComputer getGraphComputer() {
        InternalFactory<?> configuredFactory = configureGraphFactory(keyspace, location, REST.GraphConfig.COMPUTER);
        Graph graph = configuredFactory.getTinkerPopGraph(false);
        return new GraknComputerImpl(graph);
    }

    @Override
    public void close() throws GraphOperationException {
        int openTransactions = openTransactions(graph) + openTransactions(graphBatch);
        if(openTransactions > 0){
            LOG.warn(ErrorMessage.TRANSACTIONS_OPEN.getMessage(this.keyspace, openTransactions));
        }

        //Close the main graph connections
        if(graph != null) graph.admin().closeSession();
        if(graphBatch != null) graphBatch.admin().closeSession();
    }

    private int openTransactions(AbstractGraknGraph<?> graph){
        if(graph == null) return 0;
        return graph.numOpenTx();
    }

    /**
     * @param keyspace The keyspace of the graph
     * @param location The of where the graph is stored
     * @param graphType The type of graph to produce, default, batch, or compute
     * @return A new or existing grakn graph factory with the defined name connecting to the specified remote location
     */
    static InternalFactory<?> configureGraphFactory(String keyspace, String location, String graphType){
        if(Grakn.IN_MEMORY.equals(location)){
            return configureGraphFactoryInMemory(keyspace);
        } else {
            return configureGraphFactoryRemote(keyspace, location, graphType);
        }
    }

    /**
     *
     * @param keyspace The keyspace of the graph
     * @param engineUrl The url of engine to get the graph factory config from
     * @param graphType The type of graph to produce, default, batch, or compute
     * @return A new or existing grakn graph factory with the defined name connecting to the specified remote location
     */
    private static InternalFactory<?> configureGraphFactoryRemote(String keyspace, String engineUrl, String graphType){
        String restFactoryUri = engineUrl + INITIALISE + "?" + GRAPH_CONFIG_PARAM + "=" + graphType + "&" + KEYSPACE_PARAM + "=" + keyspace;

        Properties properties = new Properties();
        properties.putAll(read(contactEngine(restFactoryUri, REST.HttpConn.GET_METHOD)).asMap());

        return FactoryBuilder.getFactory(keyspace, engineUrl, properties);
    }

    /**
     *
     * @param keyspace The keyspace of the graph
     * @return  A new or existing grakn graph factory with the defined name holding the graph in memory
     */
    private static InternalFactory<?> configureGraphFactoryInMemory(String keyspace){
        Properties inMemoryProperties = new Properties();
        inMemoryProperties.put(AbstractGraknGraph.SHARDING_THRESHOLD, 100_000);
        inMemoryProperties.put(AbstractGraknGraph.NORMAL_CACHE_TIMEOUT_MS, 30_000);
        inMemoryProperties.put(FactoryBuilder.FACTORY_TYPE, TinkerInternalFactory.class.getName());

        return FactoryBuilder.getFactory(TinkerInternalFactory.class.getName(), keyspace, Grakn.IN_MEMORY, inMemoryProperties);
    }
}
