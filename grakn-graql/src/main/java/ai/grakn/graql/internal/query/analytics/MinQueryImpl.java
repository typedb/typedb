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
import ai.grakn.graql.analytics.MinQuery;
import ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram;
import ai.grakn.graql.internal.analytics.MinMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class MinQueryImpl extends AbstractStatisticsQuery<Optional<Number>> implements MinQuery {

    MinQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public Optional<Number> execute() {
        LOGGER.info("MinMapReduce is called");
        long startTime = System.currentTimeMillis();

        initSubGraph();
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeLabels);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeLabels)) return Optional.empty();
        Set<TypeLabel> allSubTypes = getCombinedSubTypes();

        ComputerResult result = getGraphComputer().compute(
                new DegreeStatisticsVertexProgram(allSubTypes, statisticsResourceTypeLabels),
                new MinMapReduce(statisticsResourceTypeLabels, dataType));
        Map<Serializable, Number> min = result.memory().get(MinMapReduce.class.getName());

        LOGGER.debug("Min = " + min.get(MapReduce.NullObject.instance()));
        LOGGER.info("MinMapReduce is done in " + (System.currentTimeMillis() - startTime) + " ms");
        return Optional.of(min.get(MapReduce.NullObject.instance()));
    }

    @Override
    public MinQuery of(String... resourceTypeLabels) {
        return (MinQuery) setStatisticsResourceType(resourceTypeLabels);
    }

    @Override
    public MinQuery of(Collection<TypeLabel> resourceTypeLabels) {
        return (MinQuery) setStatisticsResourceType(resourceTypeLabels);
    }

    @Override
    public MinQuery in(String... subTypeLabels) {
        return (MinQuery) super.in(subTypeLabels);
    }

    @Override
    public MinQuery in(Collection<TypeLabel> subTypeLabels) {
        return (MinQuery) super.in(subTypeLabels);
    }

    @Override
    public MinQuery withGraph(GraknGraph graph) {
        return (MinQuery) super.withGraph(graph);
    }

    @Override
    String getName() {
        return "min";
    }
}
