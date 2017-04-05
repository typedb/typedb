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
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.analytics.MedianQuery;
import ai.grakn.graql.internal.analytics.MedianVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

class MedianQueryImpl extends AbstractStatisticsQuery<Optional<Number>> implements MedianQuery {

    MedianQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public Optional<Number> execute() {
        LOGGER.info("MedianVertexProgram is called");
        long startTime = System.currentTimeMillis();

        initSubGraph();
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeLabels);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeLabels)) return Optional.empty();
        Set<TypeLabel> allSubTypes = getCombinedSubTypes();

        ComputerResult result = getGraphComputer().compute(
                new MedianVertexProgram(allSubTypes, statisticsResourceTypeLabels, dataType));

        Number finalResult = result.memory().get(MedianVertexProgram.MEDIAN);
        LOGGER.debug("Median = " + finalResult);

        LOGGER.info("MedianVertexProgram is done in " + (System.currentTimeMillis() - startTime) + " ms");
        return Optional.of(finalResult);
    }

    @Override
    public MedianQuery of(String... resourceTypeLabels) {
        return (MedianQuery) setStatisticsResourceType(resourceTypeLabels);
    }

    @Override
    public MedianQuery of(Collection<TypeLabel> resourceTypeLabels) {
        return (MedianQuery) setStatisticsResourceType(resourceTypeLabels);
    }

    @Override
    public MedianQuery in(String... subTypeLabels) {
        return (MedianQuery) super.in(subTypeLabels);
    }

    @Override
    public MedianQuery in(Collection<TypeLabel> subTypeLabels) {
        return (MedianQuery) super.in(subTypeLabels);
    }

    @Override
    public MedianQuery withGraph(GraknGraph graph) {
        return (MedianQuery) super.withGraph(graph);
    }

    @Override
    String getName() {
        return "median";
    }
}
