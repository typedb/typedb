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
import ai.grakn.graql.analytics.StdQuery;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import ai.grakn.graql.internal.analytics.StdMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class StdQueryImpl extends AbstractStatisticsQuery<Optional<Double>> implements StdQuery {

    StdQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public Optional<Double> execute() {
        LOGGER.info("StdMapReduce is called");
        initSubGraph();
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeNames);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeNames)) return Optional.empty();
        Set<TypeName> allSubTypes = getCombinedSubTypes();

        ComputerResult result = getGraphComputer().compute(
                new DegreeVertexProgram(allSubTypes, statisticsResourceTypeNames),
                new StdMapReduce(statisticsResourceTypeNames, dataType));
        Map<String, Map<String, Double>> std = result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
        Map<String, Double> stdTuple = std.get(StdMapReduce.MEMORY_KEY);
        double squareSum = stdTuple.get(StdMapReduce.SQUARE_SUM);
        double sum = stdTuple.get(StdMapReduce.SUM);
        double count = stdTuple.get(StdMapReduce.COUNT);
        LOGGER.info("StdMapReduce is done");
        return Optional.of(Math.sqrt(squareSum / count - (sum / count) * (sum / count)));
    }

    @Override
    public StdQuery of(TypeName... resourceTypeNames) {
        return (StdQuery) setStatisticsResourceType(resourceTypeNames);
    }

    @Override
    public StdQuery of(Collection<TypeName> resourceTypeNames) {
        return (StdQuery) setStatisticsResourceType(resourceTypeNames);
    }

    @Override
    public StdQuery in(TypeName... subTypeNames) {
        return (StdQuery) super.in(subTypeNames);
    }

    @Override
    public StdQuery in(Collection<TypeName> subTypeNames) {
        return (StdQuery) super.in(subTypeNames);
    }

    @Override
    public StdQuery withGraph(GraknGraph graph) {
        return (StdQuery) super.withGraph(graph);
    }

    @Override
    String getName() {
        return "std";
    }
}
