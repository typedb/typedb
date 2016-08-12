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

package io.mindmaps;

import io.mindmaps.constants.ErrorMessage;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class MindmapsComputer {
    private final Graph graph;
    private final Class<? extends GraphComputer> graphComputer;

    public MindmapsComputer(Graph graph, String graphComputerType){
        this.graph = graph;
        this.graphComputer = getGraphComputer(graphComputerType);
    }

    public void compute(VertexProgram program){
        //TODO: Run the program I guess ?
    }

    /**
     *
     * @return A graph compute supported by this mindmaps graph
     */
    private Class<? extends GraphComputer> getGraphComputer(String graphComputerType){
        try {
            return (Class<? extends GraphComputer>) Class.forName(graphComputerType);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(ErrorMessage.INVALID_COMPUTER.getMessage(graphComputerType));
        }
    }


}
