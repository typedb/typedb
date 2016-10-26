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

package io.mindmaps.graph.internal;

import io.mindmaps.MindmapsComputer;
import io.mindmaps.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.concurrent.ExecutionException;

public class MindmapsComputerImpl implements MindmapsComputer {
    protected final Graph graph;
    protected final Class<? extends GraphComputer> graphComputer;

    public MindmapsComputerImpl(Graph graph, String graphComputerType) {
        this.graph = graph;
        this.graphComputer = getGraphComputer(graphComputerType);
    }

    @Override
    public ComputerResult compute(VertexProgram program, MapReduce... mapReduces) {
        try {
            GraphComputer graphComputer = getComputer().program(program);
            for (MapReduce mapReduce : mapReduces)
                graphComputer = graphComputer.mapReduce(mapReduce);
            return graphComputer.submit().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ComputerResult compute(MapReduce mapReduce) {
        try {
            return getComputer().mapReduce(mapReduce).submit().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return A graph compute supported by this mindmaps graph
     */
    @SuppressWarnings("unchecked")
    private Class<? extends GraphComputer> getGraphComputer(String graphComputerType) {
        try {
            return (Class<? extends GraphComputer>) Class.forName(graphComputerType);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(ErrorMessage.INVALID_COMPUTER.getMessage(graphComputerType));
        }
    }

    protected GraphComputer getComputer() {
        return graph.compute(this.graphComputer);
    }

}
