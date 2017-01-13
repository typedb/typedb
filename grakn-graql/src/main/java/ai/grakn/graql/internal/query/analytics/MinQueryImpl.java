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
import ai.grakn.concept.TypeName;
import ai.grakn.graql.analytics.MinQuery;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import ai.grakn.graql.internal.analytics.MinMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

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
        initSubGraph();
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeNames);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeNames)) return Optional.empty();
        Set<TypeName> allSubTypes = getCombinedSubTypes();

        ComputerResult result = getGraphComputer().compute(
                new DegreeVertexProgram(allSubTypes, statisticsResourceTypeNames),
                new MinMapReduce(statisticsResourceTypeNames, dataType));
        Map<String, Number> min = result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
        LOGGER.info("MinMapReduce is done");
        return Optional.of(min.get(MinMapReduce.MEMORY_KEY));
    }

    @Override
    public MinQuery of(TypeName... resourceTypeNames) {
        return (MinQuery) setStatisticsResourceType(resourceTypeNames);
    }

    @Override
    public MinQuery of(Collection<TypeName> resourceTypeNames) {
        return (MinQuery) setStatisticsResourceType(resourceTypeNames);
    }

    @Override
    public MinQuery in(TypeName... subTypeNames) {
        return (MinQuery) super.in(subTypeNames);
    }

    @Override
    public MinQuery in(Collection<TypeName> subTypeNames) {
        return (MinQuery) super.in(subTypeNames);
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
