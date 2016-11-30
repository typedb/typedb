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

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.internal.analytics.ClusterMemberMapReduce;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import ai.grakn.graql.internal.analytics.ShortestPathVertexProgram;
import ai.grakn.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PathQueryImpl extends AbstractComputeQuery<List<Concept>> implements PathQuery {

    private String sourceId = null;
    private String destinationId = null;

    public PathQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public List<Concept> execute() {
        LOGGER.info("ShortestPathVertexProgram is called");
        if (sourceId == null) throw new IllegalStateException(ErrorMessage.NO_SOURCE.getMessage());
        if (destinationId == null) throw new IllegalStateException(ErrorMessage.NO_DESTINATION.getMessage());
        initSubGraph();
        if (!verticesExistInSubgraph(sourceId, destinationId))
            throw new IllegalStateException(ErrorMessage.INSTANCE_DOES_NOT_EXIST.getMessage());
        if (sourceId.equals(destinationId)) return Collections.singletonList(graph.get().getConcept(sourceId));
        ComputerResult result = getGraphComputer().compute(
                new ShortestPathVertexProgram(subTypeNames, sourceId, destinationId),
                new ClusterMemberMapReduce(subTypeNames, ShortestPathVertexProgram.FOUND_IN_ITERATION));
        Map<Integer, Set<String>> map = result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);

        List<String> path = new ArrayList<>();
        path.add(sourceId);
        path.addAll(map.entrySet().stream()
                .sorted(Comparator.comparingInt(Map.Entry::getKey))
                .map(pair -> pair.getValue().iterator().next())
                .collect(Collectors.toList()));
        path.add(destinationId);
        LOGGER.info("ShortestPathVertexProgram is done");
        return path.stream().map(graph.get()::<Instance>getConcept).collect(Collectors.toList());
    }

    @Override
    public PathQuery from(String sourceId) {
        this.sourceId = sourceId;
        return this;
    }

    @Override
    public PathQuery to(String destinationId) {
        this.destinationId = destinationId;
        return this;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public PathQuery in(String... subTypeNames) {
        return (PathQuery) super.in(subTypeNames);
    }

    @Override
    public PathQuery in(Collection<String> subTypeNames) {
        return (PathQuery) super.in(subTypeNames);
    }

    @Override
    public PathQuery withGraph(GraknGraph graph) {
        return (PathQuery) super.withGraph(graph);
    }
}
