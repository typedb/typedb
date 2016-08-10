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

import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.implementation.ErrorMessage;
import io.mindmaps.core.implementation.MindmapsTitanHadoopGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

class MindmapsTitanHadoopGraphFactory implements MindmapsGraphFactory {
    protected final Logger LOG = LoggerFactory.getLogger(MindmapsTitanHadoopGraphFactory.class);
    private final Map<String, MindmapsTitanHadoopGraph> openGraphs;
    private final static String KEYSPACE_PROPERTY = "titanmr.ioformat.conf.storage.cassandra.keyspace";
    private final static String GRAPH_COMPUTER = "org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer";

    public MindmapsTitanHadoopGraphFactory(){
        openGraphs = new HashMap<>();
    }

    @Override
    public MindmapsGraph getGraph(String name, String address, String pathToConfig) {
        if(address != null){
            LOG.warn(ErrorMessage.CONFIG_IGNORED.getMessage("address", address));
        }

        if(pathToConfig == null){
            throw new IllegalArgumentException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(pathToConfig));
        }

        if(!openGraphs.containsKey(name)){
            Graph graph = GraphFactory.open(pathToConfig);
            graph.configuration().setProperty(KEYSPACE_PROPERTY, name);
            openGraphs.put(name, new MindmapsTitanHadoopGraph(graph, address, GRAPH_COMPUTER));
        }

        return openGraphs.get(name);
    }
}
