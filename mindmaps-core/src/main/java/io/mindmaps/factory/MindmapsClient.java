/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.factory;

import io.mindmaps.MindmapsComputer;
import io.mindmaps.MindmapsComputerImpl;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.constants.RESTUtil;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.implementation.EngineCommunicator;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.PropertyResourceBundle;

import static io.mindmaps.constants.RESTUtil.Request.GRAPH_CONFIG_PARAM;
import static io.mindmaps.constants.RESTUtil.WebPath.GRAPH_FACTORY_URI;

/**
 * A client for creating a mindmaps graph from a running engine.
 * This is to abstract away factories and the backend from the user.
 * The deployer of engine decides on the backend and this class will handle producing the correct graphs.
 */
public class MindmapsClient {
    private static final String DEFAULT_URI = "localhost:4567";
    private static final String FACTORY = "factory.internal";
    private static final String COMPUTER = "graph.computer";
    private static final Map<String, MindmapsGraphFactory> openFactories = new HashMap<>();

    /**
     *
     * @param name The desired name for the mindmaps graph
     * @return A new or existing mindmaps graph with the defined name
     */
    public static MindmapsGraph getGraph(String name){
        return getGraph(name, DEFAULT_URI);
    }

    /**
     *
     * @param name The desired name for the mindmaps graph
     * @return A new or existing mindmaps graph with the defined name connecting to the specified remote uri with batch loading enabled
     */
    public static MindmapsGraph getGraphBatchLoading(String name){
        return getGraphBatchLoading(name, DEFAULT_URI);
    }

    /**
     *
     * @param name The desired name for the mindmaps graph
     * @param uri The remote uri fo where engine is located
     * @return A new or existing mindmaps graph with the defined name connecting to the specified remote uri
     */
    public static MindmapsGraph getGraph(String name, String uri){
        ConfigureFactory configuredFactory = configureGraphFactory(uri, RESTUtil.GraphConfig.DEFAULT);
        return configuredFactory.factory.getGraph(name, uri, configuredFactory.path, false);
    }

    /**
     *
     * @param name The desired name for the mindmaps graph
     * @param uri The remote uri fo where engine is located
     * @return A new or existing mindmaps graph with the defined name connecting to the specified remote uri with batch loading enabled
     */
    public static MindmapsGraph getGraphBatchLoading(String name, String uri){
        ConfigureFactory configuredFactory = configureGraphFactory(uri, RESTUtil.GraphConfig.BATCH);
        return configuredFactory.factory.getGraph(name, uri, configuredFactory.path, true);
    }

    /**
     *
     * @return A new or existing mindmaps graph compute with the defined name
     */
    public static MindmapsComputer getGraphComputer() {
        return getGraphComputer(DEFAULT_URI);
    }

    /**
     *
     * @param uri The remote uri fo where engine is located
     * @return A new or existing mindmaps graph compute with the defined name
     */
    public static MindmapsComputer getGraphComputer(String uri) {
        ConfigureFactory configuredFactory = configureGraphFactory(uri, RESTUtil.GraphConfig.COMPUTER);
        Graph graph = configuredFactory.factory.getTinkerPopGraph(null, uri, configuredFactory.path, false);
        return new MindmapsComputerImpl(graph, configuredFactory.graphComputer);
    }

    /**
     *
     * @param uri The remote uri fo where engine is located
     * @param graphType The type of graph to produce, default, batch, or compute
     * @return A new or existing mindmaps graph with the defined name connecting to the specified remote uri
     */
    private static ConfigureFactory configureGraphFactory(String uri, String graphType){
        try {
            String restFactoryUri = uri + GRAPH_FACTORY_URI + "?" + GRAPH_CONFIG_PARAM + "=" + graphType;
            String config = EngineCommunicator.contactEngine(restFactoryUri, "GET");

            //TODO: We should make config handling generic rather than through files. Using a temp file here is a bit strange
            //Creating Temp File
            File file = File.createTempFile("mindmaps-config", ".tmp");
            String path = file.getAbsolutePath();
            BufferedWriter bw = new BufferedWriter(new FileWriter(file));
            bw.write(config);
            bw.close();

            //Creating the actual mindmaps graph using reflection to identify the factory
            FileInputStream fis = new FileInputStream(path);
            PropertyResourceBundle bundle = new PropertyResourceBundle(fis);

            String factoryType;
            String computer = null;
            try {
                factoryType = bundle.getString(FACTORY);
                if(bundle.containsKey(COMPUTER)){
                    computer = bundle.getString(COMPUTER);
                }
            } catch(MissingResourceException e){
                fis.close();
                throw new IllegalArgumentException(ErrorMessage.MISSING_FACTORY_DEFINITION.getMessage());
            }
            fis.close();
            return new ConfigureFactory(path, computer, getFactory(factoryType));
        } catch (IOException e) {
            throw new IllegalArgumentException(ErrorMessage.CONFIG_NOT_FOUND.getMessage(uri, e.getMessage()));
        }
    }


    /**
     *
     * @param factoryType The string defining which factory should be used for creating the mindmaps graph.
     *                    A valid example includes: io.mindmaps.factory.MindmapsTinkerGraphFactory
     * @return A graph factory which produces the relevant expected graph.
     */
    private static MindmapsGraphFactory getFactory(String factoryType){
        if(!openFactories.containsKey(factoryType)) {
            MindmapsGraphFactory mindmapsGraphFactory;
            try {
                mindmapsGraphFactory = (MindmapsGraphFactory) Class.forName(factoryType).newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new IllegalArgumentException(ErrorMessage.INVALID_FACTORY.getMessage(factoryType));
            }
            openFactories.put(factoryType, mindmapsGraphFactory);
        }
        return openFactories.get(factoryType);
    }


    private static class ConfigureFactory {
        String path;
        String graphComputer;
        MindmapsGraphFactory factory;

        ConfigureFactory(String path, String graphComputer, MindmapsGraphFactory factory){
            this.path = path;
            this.graphComputer = graphComputer;
            this.factory = factory;
        }
    }
}
