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

import ai.grakn.GraknGraph;
import ai.grakn.engine.util.ConfigProperties;

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

    public synchronized GraknGraph getGraph(String keyspace) {
        GraknGraph graph = FactoryBuilder.getFactory(keyspace, null, graphConfig).getGraph(false);
        graph.showImplicitConcepts(true);
        return graph;
    }
    public synchronized GraknGraph getGraphBatchLoading(String keyspace) {
        GraknGraph graph = FactoryBuilder.getFactory(keyspace, null, graphBatchConfig).getGraph(true);
        graph.showImplicitConcepts(true);
        return graph;
    }
}


