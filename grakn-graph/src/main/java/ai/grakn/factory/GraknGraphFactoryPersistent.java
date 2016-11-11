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

import ai.grakn.GraknComputer;
import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import ai.grakn.graph.internal.EngineCommunicator;
import ai.grakn.graph.internal.GraknComputerImpl;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.REST;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.PropertyResourceBundle;

import static ai.grakn.util.REST.Request.GRAPH_CONFIG_PARAM;
import static ai.grakn.util.REST.WebPath.GRAPH_FACTORY_URI;

/**
 * A client for creating a grakn graph from a running engine.
 * This is to abstract away factories and the backend from the user.
 * The deployer of engine decides on the backend and this class will handle producing the correct graphs.
 */
public class GraknGraphFactoryPersistent implements GraknGraphFactory {
    private static final String COMPUTER = "graph.computer";
    private final String uri;
    private final String keyspace;

    public GraknGraphFactoryPersistent(String keyspace, String uri){
        this.uri = uri;
        this.keyspace = keyspace;
    }

    /**
     *
     * @return A new or existing grakn graph with the defined name
     */
    public GraknGraph getGraph(){
        ConfigureFactory configuredFactory = configureGraphFactory(keyspace, uri, REST.GraphConfig.DEFAULT);
        return configuredFactory.factory.getGraph(false);
    }

    /**
     *
     * @return A new or existing grakn graph with the defined name connecting to the specified remote uri with batch loading enabled
     */
    public GraknGraph getGraphBatchLoading(){
        ConfigureFactory configuredFactory = configureGraphFactory(keyspace, uri, REST.GraphConfig.BATCH);
        return configuredFactory.factory.getGraph(true);
    }

    /**
     * @return A new or existing grakn graph compute with the defined name
     */
    public GraknComputer getGraphComputer() {
        ConfigureFactory configuredFactory = configureGraphFactory(keyspace, uri, REST.GraphConfig.COMPUTER);
        Graph graph = configuredFactory.factory.getTinkerPopGraph(false);
        return new GraknComputerImpl(graph, configuredFactory.graphComputer);
    }

    /**
     *
     * @param engineUrl The remote uri fo where engine is located
     * @param graphType The type of graph to produce, default, batch, or compute
     * @return A new or existing grakn graph with the defined name connecting to the specified remote uri
     */
    protected static ConfigureFactory configureGraphFactory(String keyspace, String engineUrl, String graphType){
        try {
            String restFactoryUri = engineUrl + GRAPH_FACTORY_URI + "?" + GRAPH_CONFIG_PARAM + "=" + graphType;
            String config = EngineCommunicator.contactEngine(restFactoryUri, REST.HttpConn.GET_METHOD);

            //TODO: We should make config handling generic rather than through files. Using a temp file here is a bit strange
            //Creating Temp File
            File file = File.createTempFile("grakn-config", ".tmp");
            String path = file.getAbsolutePath();
            BufferedWriter bw = new BufferedWriter(new FileWriter(file));
            bw.write(config);
            bw.close();

            //Creating the actual grakn graph using reflection to identify the factory
            FileInputStream fis = new FileInputStream(path);
            PropertyResourceBundle bundle = new PropertyResourceBundle(fis);
            fis.close();

            String computer = null;
            if(bundle.containsKey(COMPUTER)){
                computer = bundle.getString(COMPUTER);
            }

            return new ConfigureFactory(path, computer, FactoryBuilder.getFactory(keyspace, engineUrl, path));
        } catch (IOException e) {
            throw new IllegalArgumentException(ErrorMessage.CONFIG_NOT_FOUND.getMessage(engineUrl, e.getMessage()));
        }
    }

    static class ConfigureFactory {
        String path;
        String graphComputer;
        InternalFactory factory;

        ConfigureFactory(String path, String graphComputer, InternalFactory factory){
            this.path = path;
            this.graphComputer = graphComputer;
            this.factory = factory;
        }
    }
}
