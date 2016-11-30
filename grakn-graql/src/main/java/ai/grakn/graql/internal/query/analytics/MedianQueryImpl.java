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
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.analytics.MedianQuery;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import ai.grakn.graql.internal.analytics.MeanMapReduce;
import ai.grakn.graql.internal.analytics.MedianVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MedianQueryImpl extends AbstractStatisticsQuery<Optional<Number>> implements MedianQuery {

    public MedianQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public Optional<Number> execute() {
        LOGGER.info("MedianVertexProgram is called");
        initSubGraph();
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeNames);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeNames)) return Optional.empty();
        Set<String> allSubTypes = getCombinedSubTypes();

        ComputerResult result = getGraphComputer().compute(
                new MedianVertexProgram(allSubTypes, statisticsResourceTypeNames, dataType));
        LOGGER.info("MedianVertexProgram is done");
        return Optional.of(result.memory().get(MedianVertexProgram.MEDIAN));
    }

    @Override
    public MedianQuery of(String... resourceTypeNames) {
        return (MedianQuery) setStatisticsResourceType(resourceTypeNames);
    }

    @Override
    public MedianQuery of(Collection<String> resourceTypeNames) {
        return (MedianQuery) setStatisticsResourceType(resourceTypeNames);
    }

    @Override
    public MedianQuery in(String... subTypeNames) {
        return (MedianQuery) super.in();
    }

    @Override
    public MedianQuery in(Collection<String> subTypeNames) {
        return (MedianQuery) super.in();
    }

    @Override
    public MedianQuery withGraph(GraknGraph graph) {
        return (MedianQuery) super.withGraph(graph);
    }

}
