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
import io.mindmaps.core.implementation.MindmapsTransactionImpl;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.IOException;
import java.util.Properties;

public class GraphFactory {

    private String CONFIG;
    private String DEFAULT_NAME; //TO_DO: This should be parametrised

    private static GraphFactory instance = null;

    private MindmapsGraphFactory titanGraphFactory;


    public static synchronized GraphFactory getInstance() {
        if (instance == null) {
            instance = new GraphFactory();
        }
        return instance;
    }

    private GraphFactory() {

//        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
//        Configuration conf = ctx.getConfiguration();
//        conf.getLoggerConfig(LogManager.ROOT_LOGGER_NAME).setLevel(Level.ERROR);


        titanGraphFactory = new MindmapsTitanGraphFactory();
        Properties prop = new Properties();
        try {
            prop.load(getClass().getClassLoader().getResourceAsStream("application.properties"));
            CONFIG = prop.getProperty("graphdatabase.config");
            DEFAULT_NAME = prop.getProperty("graphdatabase.name");
            System.out.println("hello config " + CONFIG);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public MindmapsTransactionImpl buildMindmapsGraphBatchLoading() {
        MindmapsGraph graph = buildGraph(DEFAULT_NAME, CONFIG);
        graph.enableBatchLoading();
        return (MindmapsTransactionImpl) graph.newTransaction();
    }

    public MindmapsTransactionImpl buildMindmapsGraph() {
        return (MindmapsTransactionImpl) buildGraph(DEFAULT_NAME, CONFIG).newTransaction();
    }

    private synchronized MindmapsGraph buildGraph(String name, String config) {
        MindmapsGraph mindmapsGraph = titanGraphFactory.getGraph(name, null, config);
        Graph graph = mindmapsGraph.getGraph();

        return mindmapsGraph;
    }
}
