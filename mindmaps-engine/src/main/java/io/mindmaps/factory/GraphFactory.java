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

import io.mindmaps.util.ConfigProperties;
import io.mindmaps.core.dao.MindmapsGraph;

public class GraphFactory {

    private String graphConfig;

    private static GraphFactory instance = null;

    private MindmapsGraphFactory titanGraphFactory;


    public static synchronized GraphFactory getInstance() {
        if (instance == null) {
            instance = new GraphFactory();
        }
        return instance;
    }

    private GraphFactory() {
        titanGraphFactory = new MindmapsTitanGraphFactory();
        graphConfig = ConfigProperties.getInstance().getProperty(ConfigProperties.GRAPH_CONFIG_PROPERTY);
    }

    public synchronized MindmapsGraph getGraph(String name) {
        return titanGraphFactory.getGraph(name, null, graphConfig);
    }

    public synchronized MindmapsGraph getGraphBatchLoading(String name) {
        MindmapsGraph graph = titanGraphFactory.getGraph(name, null, graphConfig);
        graph.enableBatchLoading();
        return graph;
    }
}


