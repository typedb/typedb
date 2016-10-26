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

package io.mindmaps.test.graql.analytics;

import io.mindmaps.graph.internal.MindmapsComputerImpl;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 *
 */
public class MindmapsComputerMock extends MindmapsComputerImpl {

    int numberOfWorkers;

    public MindmapsComputerMock(Graph graph, String graphComputerType) {
        super(graph, graphComputerType);
    }

    public MindmapsComputerMock(Graph graph, String graphComputerType, int numberOfWorkers) {
        super(graph, graphComputerType);
        this.numberOfWorkers = numberOfWorkers;

    }

    @Override
    protected GraphComputer getComputer() {
        return graph.compute(this.graphComputer).workers(numberOfWorkers);
    }
}
