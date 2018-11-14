/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server;

import grakn.core.graql.concept.LabelId;
import grakn.core.graql.internal.Schema;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Set;

/**
 * <p>
 * {@link GraphComputer} Used For Analytics Algorithms
 * </p>
 * <p>
 * Wraps a Tinkerpop {@link GraphComputer} which enables the execution of pregel programs.
 * These programs are defined either via a {@link MapReduce} or a {@link VertexProgram}.
 * </p>
 * <p>
 * A {@link VertexProgram} is a computation executed on each vertex in parallel.
 * Vertices communicate with each other through message passing.
 * </p>
 * <p>
 * {@link MapReduce} processed the vertices in a parallel manner by aggregating values emitted by vertices.
 * MapReduce can be executed alone or used to collect the results after executing a VertexProgram.
 * </p>
 *
 */
public interface GraknComputer {

    /**
     * Execute the given vertex program using a graph computer.
     *
     * @param includesRolePlayerEdges whether the graph computer should include {@link Schema.EdgeLabel#ROLE_PLAYER} edges
     * @param types                   instance types in the subgraph
     * @param program                 the vertex program
     * @param mapReduce               a list of mapReduce job
     * @return the result of the computation
     * @see ComputerResult
     */
    @CheckReturnValue
    ComputerResult compute(@Nullable VertexProgram program, @Nullable MapReduce mapReduce,
                           @Nullable Set<LabelId> types, Boolean includesRolePlayerEdges);

    /**
     * Execute the given vertex program using a graph computer.
     *
     * @param types     instance types in the subgraph
     * @param program   the vertex program
     * @param mapReduce a list of mapReduce job
     * @return the result of the computation
     * @see ComputerResult
     */
    @CheckReturnValue
    ComputerResult compute(@Nullable VertexProgram program, @Nullable MapReduce mapReduce,
                           @Nullable Set<LabelId> types);

    /**
     * Kill all the jobs the graph computer has
     */
    void killJobs();
}
