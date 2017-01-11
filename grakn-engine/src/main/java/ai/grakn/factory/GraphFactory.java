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
import ai.grakn.GraknGraph;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.util.ErrorMessage;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class GraphFactory {
    private final Properties properties;
    private static GraphFactory instance = null;


    public static synchronized GraphFactory getInstance() {
        if (instance == null) {
            instance = new GraphFactory();
        }
        return instance;
    }

    private GraphFactory() {
        properties = new Properties();
        String pathToConfig = ConfigProperties.getInstance().getPath(ConfigProperties.GRAPH_CONFIG_PROPERTY);

        try(FileInputStream input = new FileInputStream(pathToConfig)){
            properties.load(input);
            System.out.println("Path to graph config is: " + pathToConfig);
            System.out.println("Properties are: " + properties);
        } catch (IOException e) {
            throw new RuntimeException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(pathToConfig), e);
        }
    }

    public synchronized GraknGraph getGraph(String keyspace) {
        return FactoryBuilder.getFactory(keyspace, Grakn.DEFAULT_URI, properties).getGraph(false);
    }

    public synchronized void refershConnections(){
        FactoryBuilder.refresh();
    }
}


