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

import ai.grakn.graph.internal.GraknComputerImpl;
import ai.grakn.graph.internal.computer.GraknSparkComputer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 *
 */
public class GraknComputerMock extends GraknComputerImpl {
    private final Graph graph;
    private int numberOfWorkers;

    public GraknComputerMock(Graph graph, int numberOfWorkers) {
        super(graph);
        this.numberOfWorkers = numberOfWorkers;
        this.graph = graph;
    }

    @Override
    protected GraphComputer getGraphComputer() {
        return graph.compute(getGraphComputerClass(GraknSparkComputer.class.getName())).workers(numberOfWorkers);
    }
}
