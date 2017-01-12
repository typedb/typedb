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
import ai.grakn.graql.analytics.MaxQuery;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import ai.grakn.graql.internal.analytics.MaxMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

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
        initSubGraph();
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeNames);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeNames)) return Optional.empty();
        Set<TypeName> allSubTypes = getCombinedSubTypes();

        ComputerResult result = getGraphComputer().compute(
                new DegreeVertexProgram(allSubTypes, statisticsResourceTypeNames),
                new MaxMapReduce(statisticsResourceTypeNames, dataType));
        Map<String, Number> max = result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
        LOGGER.info("MaxMapReduce is done");
        return Optional.of(max.get(MaxMapReduce.MEMORY_KEY));
    }

    @Override
    public MaxQuery of(TypeName... resourceTypeNames) {
        return (MaxQuery) setStatisticsResourceType(resourceTypeNames);
    }

    @Override
    public MaxQuery of(Collection<TypeName> resourceTypeNames) {
        return (MaxQuery) setStatisticsResourceType(resourceTypeNames);
    }

    @Override
    public MaxQuery in(TypeName... subTypeNames) {
        return (MaxQuery) super.in(subTypeNames);
    }

    @Override
    public MaxQuery in(Collection<TypeName> subTypeNames) {
        return (MaxQuery) super.in(subTypeNames);
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
