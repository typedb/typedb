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

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.PropertyResourceBundle;

public class MindmapsClient {
    private static final String DEFAULT_PROTOCOL = "http://";
    private static final String DEFAULT_URI = "localhost";
    private static final String REST_END_POINT = ":4567/graph_factory";
    private static final Map<String, MindmapsGraphFactory> openFactories = new HashMap<>();

    public static MindmapsGraph getGraph(String name){
        return getGraph(name, DEFAULT_URI);
    }

    public static MindmapsGraph getGraph(String name, String uri){
        try {
            String restFactoryUri = DEFAULT_PROTOCOL + uri + REST_END_POINT;
            URL url = new URL(restFactoryUri);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            if(connection.getResponseCode() != 200){
                throw new IllegalArgumentException(ErrorMessage.INVALID_ENGINE_RESPONSE.getMessage(uri, connection.getResponseCode()));
            }

            //Reading from Connection
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append("\n").append(line);
            }
            br.close();
            String config = sb.toString();

            //TODO: We should make config handling generic rather than through files.
            //Creating Temp File
            File file = File.createTempFile("mindmaps-config", ".tmp");
            String path = file.getAbsolutePath();
            BufferedWriter bw = new BufferedWriter(new FileWriter(file));
            bw.write(config);
            bw.close();

            //Creating the actual mindmaps graph using reflection
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
