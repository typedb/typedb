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
import io.mindmaps.core.implementation.MindmapsTinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A graph factory which provides a mindmaps graph with a tinker graph backend.
 */
class MindmapsTinkerGraphFactory implements MindmapsGraphFactory {
    protected final Logger LOG = LoggerFactory.getLogger(MindmapsTinkerGraphFactory.class);
    private Map<String, MindmapsGraph> openGraphs;

    public MindmapsTinkerGraphFactory(){
        openGraphs = new HashMap<>();
    }

    /**
     *
     * @param name The name of the graph we should be initialising
     * @param address The address of where the backend is. Defaults to localhost if null
     * @param pathToConfig Path to file storing optional configuration parameters. Uses defaults if left null
     * @return  An instance of Mindmaps graph with a tinker graph backend
     */
    @Override
    public MindmapsGraph getGraph(String name, String address, String pathToConfig) {
        LOG.warn("In memory Tinkergraph ignores the address [" + address + "] and config path [" + pathToConfig + "]parameters");

        if(!openGraphs.containsKey(name)){
            openGraphs.put(name, new MindmapsTinkerGraph());
        }

        return openGraphs.get(name);
    }
}
