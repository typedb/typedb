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

package ai.grakn.graph.internal.computer;

import ai.grakn.GraknComputer;
import ai.grakn.concept.LabelId;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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

    public GraknComputerImpl(Graph graph) {
        this.graph = graph;
        if (graph instanceof TinkerGraph) {
            graphComputerClass = TinkerGraphComputer.class;
        } else {
            graphComputerClass = GraknSparkComputer.class;
        }
    }

    @Override
    public ComputerResult compute(Boolean includesShortcut, Set<LabelId> types,
                                  VertexProgram program, MapReduce... mapReduces) {
        try {
            if (program != null) graphComputer = getGraphComputer().program(program);
            for (MapReduce mapReduce : mapReduces)
                graphComputer = graphComputer.mapReduce(mapReduce);
            applyFilters(types, includesShortcut);
            return graphComputer.submit().get();
        } catch (InterruptedException | ExecutionException e) {
            throw asRuntimeException(e);
        }
    }

    @Override
    public ComputerResult compute(Set<LabelId> types, VertexProgram program, MapReduce... mapReduces) {
        return compute(true, types, program, mapReduces);
    }

    @Override
    public ComputerResult compute(VertexProgram program, MapReduce... mapReduces) {
        // only for internal tasks
        return compute(Collections.emptySet(), program, mapReduces);
    }

    @Override
    public ComputerResult compute(MapReduce mapReduce) {
        return compute(null, mapReduce);
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

    private void applyFilters(Set<LabelId> types, boolean includesShortcut) {
        if (types == null || types.isEmpty()) return;
        Set<Integer> labelIds = types.stream().map(LabelId::getValue).collect(Collectors.toSet());

        Traversal<Vertex, Vertex> vertexFilter =
                __.has(Schema.VertexProperty.THING_TYPE_LABEL_ID.name(), P.within(labelIds));

        Traversal<Vertex, Edge> edgeFilter = includesShortcut ?
                __.union(
                        __.bothE(Schema.EdgeLabel.SHORTCUT.getLabel()),
                        __.bothE(Schema.EdgeLabel.RESOURCE.getLabel())
                                .has(Schema.EdgeProperty.RELATION_TYPE_LABEL_ID.name(), P.within(labelIds))) :
                __.union(
                        __.bothE(Schema.EdgeLabel.RESOURCE.getLabel())
                                .has(Schema.EdgeProperty.RELATION_TYPE_LABEL_ID.name(), P.within(labelIds)));

        graphComputer.vertices(vertexFilter).edges(edgeFilter);
    }
}
