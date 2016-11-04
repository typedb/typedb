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
import io.mindmaps.graph.internal.MindmapsComputerImpl;

/**
 * A client for creating a mindmaps graph from a running engine.
 * This is to abstract away factories and the backend from the user.
 * The deployer of engine decides on the backend and this class will handle producing the correct graphs.
 */
public class MindmapsGraphFactoryInMemory implements MindmapsGraphFactory {
    private static final String TINKER_GRAPH_COMPUTER = "org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer";
    private final TinkerInternalFactory factory;

    public MindmapsGraphFactoryInMemory(String keyspace, String ignored){
        factory = new TinkerInternalFactory(keyspace, null, null);
    }

    @Override
    public MindmapsGraph getGraph() {
        return factory.getGraph(false);
    }

    @Override
    public MindmapsGraph getGraphBatchLoading() {
        return factory.getGraph(true);
    }

    @Override
    public MindmapsComputer getGraphComputer() {
        return new MindmapsComputerImpl(factory.getTinkerPopGraph(false), TINKER_GRAPH_COMPUTER);
    }
}
