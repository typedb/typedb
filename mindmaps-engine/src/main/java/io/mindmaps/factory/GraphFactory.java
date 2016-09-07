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

import io.mindmaps.MindmapsGraph;
import io.mindmaps.engine.util.ConfigProperties;

public class GraphFactory {
    private String graphConfig;
    private String graphBatchConfig;
    private static GraphFactory instance = null;


    public static synchronized GraphFactory getInstance() {
        if (instance == null) {
            instance = new GraphFactory();
        }
        return instance;
    }

    private GraphFactory() {
        graphConfig = ConfigProperties.getInstance().getPath(ConfigProperties.GRAPH_CONFIG_PROPERTY);
        graphBatchConfig = ConfigProperties.getInstance().getPath(ConfigProperties.GRAPH_BATCH_CONFIG_PROPERTY);
    }

    public synchronized MindmapsGraph getGraph(String name) {
        return MindmapsFactoryBuilder.getFactory(graphConfig).getGraph(name, null, graphConfig, false);
    }
    public synchronized MindmapsGraph getGraphBatchLoading(String name) {
        return MindmapsFactoryBuilder.getFactory(graphBatchConfig).getGraph(name, null, graphBatchConfig, true);
    }
}


