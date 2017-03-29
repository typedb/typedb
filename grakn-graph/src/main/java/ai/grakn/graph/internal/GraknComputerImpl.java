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

package ai.grakn.graph.internal;

import ai.grakn.GraknComputer;
import ai.grakn.graph.internal.computer.GraknSparkComputer;
import ai.grakn.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.concurrent.ExecutionException;

/**
 * <p>
 * Graph Computer Used For Analytics Algorithms
 * </p>
 * <p>
 * <p>
 * Wraps a Tinkerpop {@link GraphComputer} which enables the execution of pregel programs.
 * These programs are defined either via a {@link MapReduce} or a {@link VertexProgram}.
 * <p>
 * A {@link VertexProgram} is a computation executed on each vertex in parallel.
 * Vertices communicate with each other through message passing.
 * <p>
 * {@link MapReduce} processed the vertices in a parallel manner by aggregating values emitted by vertices.
 * MapReduce can be executed alone or used to collect the results after executing a VertexProgram.
 * </p>
 *
 * @author duckofyork
 * @author sheldonkhall
 * @author fppt
 */
public class GraknComputerImpl implements GraknComputer {
    private final Graph graph;
    private final Class<? extends GraphComputer> graphComputerClass;
    private GraphComputer graphComputer = null;

    public GraknComputerImpl(Graph graph, String graphComputerType) {
        this.graph = graph;
        this.graphComputerClass = getGraphComputerClass(graphComputerType);
    }

    @Override
    public ComputerResult compute(VertexProgram program, MapReduce... mapReduces) {
        try {
            graphComputer = getGraphComputer().program(program);
            for (MapReduce mapReduce : mapReduces)
                graphComputer = graphComputer.mapReduce(mapReduce);
            return graphComputer.submit().get();
        } catch (InterruptedException | ExecutionException e) {
            throw asRuntimeException(e);
        }
    }

    @Override
    public ComputerResult compute(MapReduce mapReduce) {
        try {
            graphComputer = getGraphComputer().mapReduce(mapReduce);
            return graphComputer.submit().get();
        } catch (InterruptedException | ExecutionException e) {
            throw asRuntimeException(e);
        }
    }

    @Override
    public void killJobs() {
        if (graphComputer != null && graphComputerClass.equals(GraknSparkComputer.class)) {
            ((GraknSparkComputer) graphComputer).cancelJobs();
        }
    }

    private RuntimeException asRuntimeException(Throwable throwable) {
        Throwable cause = throwable.getCause();
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        } else {
            return new RuntimeException(cause);
        }
    }

    /**
     * @return A graph compute supported by this grakn graph
     */
    @SuppressWarnings("unchecked")
    protected Class<? extends GraphComputer> getGraphComputerClass(String graphComputerType) {
        try {
            return (Class<? extends GraphComputer>) Class.forName(graphComputerType);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(ErrorMessage.INVALID_COMPUTER.getMessage(graphComputerType));
        }
    }

    protected GraphComputer getGraphComputer() {
        return graph.compute(this.graphComputerClass);
    }

}
