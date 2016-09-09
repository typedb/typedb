/*
 *  MindmapsDB - A Distributed Semantic Database
 *  Copyright (C) 2016  Mindmaps Research Ltd
 *
 *  MindmapsDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  MindmapsDB is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps;

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;

/**
 * Encapsulates a tinkerpop graph computer and provides methods to execute OLAP tasks.
 */
public interface MindmapsComputer {

    /**
     * Execute the given vertex program using a graph computer.
     *
     * @param program   the vertex program
     * @return          the result of the computation
     */
    ComputerResult compute(VertexProgram program);

    /**
     * Execute the given map reduce job using a graph computer.
     *
     * @param mapReduce the map reduce job
     * @return          the result of the computation
     */
    ComputerResult compute(MapReduce mapReduce);
}
