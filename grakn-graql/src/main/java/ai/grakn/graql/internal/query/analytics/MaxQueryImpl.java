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
import ai.grakn.graql.analytics.MaxQuery;
import ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram;
import ai.grakn.graql.internal.analytics.MaxMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class MaxQueryImpl extends AbstractStatisticsQuery<Optional<Number>> implements MaxQuery {

    MaxQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public Optional<Number> execute() {
        LOGGER.info("MaxMapReduce is called");
        long startTime = System.currentTimeMillis();

        initSubGraph();
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeLabels);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeLabels)) return Optional.empty();
        Set<TypeLabel> allSubTypes = getCombinedSubTypes();

        ComputerResult result = getGraphComputer().compute(
                new DegreeStatisticsVertexProgram(allSubTypes, statisticsResourceTypeLabels),
                new MaxMapReduce(statisticsResourceTypeLabels, dataType));
        Map<Serializable, Number> max = result.memory().get(MaxMapReduce.class.getName());

        LOGGER.debug("Max = " + max.get(MapReduce.NullObject.instance()));
        LOGGER.info("MaxMapReduce is done in " + (System.currentTimeMillis() - startTime) + " ms");
        return Optional.of(max.get(MapReduce.NullObject.instance()));
    }

    @Override
    public MaxQuery of(String... resourceTypeLabels) {
        return (MaxQuery) setStatisticsResourceType(resourceTypeLabels);
    }

    @Override
    public MaxQuery of(Collection<TypeLabel> resourceTypeLabels) {
        return (MaxQuery) setStatisticsResourceType(resourceTypeLabels);
    }

    @Override
    public MaxQuery in(String... subTypeLabels) {
        return (MaxQuery) super.in(subTypeLabels);
    }

    @Override
    public MaxQuery in(Collection<TypeLabel> subTypeLabels) {
        return (MaxQuery) super.in(subTypeLabels);
    }

    @Override
    public MaxQuery withGraph(GraknGraph graph) {
        return (MaxQuery) super.withGraph(graph);
    }

    @Override
    String getName() {
        return "max";
    }
}
