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

import ai.grakn.GraknComputer;
import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import ai.grakn.graph.internal.GraknComputerImpl;

/**
 * A client for creating a ai graph from a running engine.
 * This is to abstract away factories and the backend from the user.
 * The deployer of engine decides on the backend and this class will handle producing the correct graphs.
 */
public class GraknGraphFactoryInMemory implements GraknGraphFactory {
    private static final String TINKER_GRAPH_COMPUTER = "org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer";
    private final TinkerInternalFactory factory;

    public GraknGraphFactoryInMemory(String keyspace, String ignored){
        factory = new TinkerInternalFactory(keyspace, null, null);
    }

    @Override
    public GraknGraph getGraph() {
        return factory.getGraph(false);
    }

    @Override
    public GraknGraph getGraphBatchLoading() {
        return factory.getGraph(true);
    }

    @Override
    public GraknComputer getGraphComputer() {
        return new GraknComputerImpl(factory.getTinkerPopGraph(false), TINKER_GRAPH_COMPUTER);
    }
}
