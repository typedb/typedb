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

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.implementation.EngineCommunicator;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.PropertyResourceBundle;

/**
 * A client for creating a mindmaps graph from a running engine.
 * This is to abstract away factories and the backend from the user.
 * The deployer of engine decides on the backend and this class will handle producing the correct graphs.
 */
public class MindmapsClient {
    private static final String DEFAULT_URI = "localhost:4567";
    private static final String REST_END_POINT = "/graph_factory";
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
     * @param uri The remote uri fo where engine is located
     * @return A new or existing mindmaps graph with the defined name connecting to the specified remote uri
     */
    public static MindmapsGraph getGraph(String name, String uri){
        try {
            String restFactoryUri = uri + REST_END_POINT;
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
            try {
                factoryType = bundle.getString("factory.internal");
            } catch(MissingResourceException e){
                throw new IllegalArgumentException(ErrorMessage.MISSING_FACTORY_DEFINITION.getMessage());
            }

            return getFactory(factoryType).getGraph(name, uri, path);
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
}
