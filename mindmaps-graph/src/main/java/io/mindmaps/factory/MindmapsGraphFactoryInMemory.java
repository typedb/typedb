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
import io.mindmaps.MindmapsGraph;
import io.mindmaps.MindmapsGraphFactory;
import io.mindmaps.util.ErrorMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * A client for creating a mindmaps graph from a running engine.
 * This is to abstract away factories and the backend from the user.
 * The deployer of engine decides on the backend and this class will handle producing the correct graphs.
 */
public class MindmapsGraphFactoryInMemory implements MindmapsGraphFactory {
    private static final Map<String, MindmapsTinkerInternalFactory> inMemoryFactories = new HashMap<>();
    private static MindmapsGraphFactoryInMemory instance;

    private MindmapsGraphFactoryInMemory(){}

    public static MindmapsGraphFactoryInMemory getInstance(){
        if(instance == null){
            instance = new MindmapsGraphFactoryInMemory();
        }
        return instance;
    }

    @Override
    public MindmapsGraph getGraph(String keyspace) {
        return getFactory(keyspace).getGraph(false);
    }

    @Override
    public MindmapsGraph getGraphBatchLoading(String keyspace) {
        return getFactory(keyspace).getGraph(true);
    }

    private MindmapsTinkerInternalFactory getFactory(String keyspace){
        return inMemoryFactories.computeIfAbsent(keyspace, (key) -> new MindmapsTinkerInternalFactory(keyspace, null, null));
    }

    @Override
    public MindmapsComputer getGraphComputer(String name) {
        throw new UnsupportedOperationException(ErrorMessage.UNSUPPORTED_GRAPH.getMessage("in-memory", "graph computer"));
    }
}
