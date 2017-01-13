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
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import ai.grakn.graql.internal.analytics.MeanMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class MeanQueryImpl extends AbstractStatisticsQuery<Optional<Double>> implements MeanQuery {

    MeanQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public Optional<Double> execute() {
        LOGGER.info("MeanMapReduce is called");
        initSubGraph();
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeNames);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeNames)) return Optional.empty();
        Set<TypeName> allSubTypes = getCombinedSubTypes();

        ComputerResult result = getGraphComputer().compute(
                new DegreeVertexProgram(allSubTypes, statisticsResourceTypeNames),
                new MeanMapReduce(statisticsResourceTypeNames, dataType));
        Map<String, Map<String, Double>> mean = result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
        Map<String, Double> meanPair = mean.get(MeanMapReduce.MEMORY_KEY);
        LOGGER.info("MeanMapReduce is done");
        return Optional.of(meanPair.get(MeanMapReduce.SUM) / meanPair.get(MeanMapReduce.COUNT));
    }

    @Override
    public MeanQuery of(TypeName... resourceTypeNames) {
        return (MeanQuery) setStatisticsResourceType(resourceTypeNames);
    }

    @Override
    public MeanQuery of(Collection<TypeName> resourceTypeNames) {
        return (MeanQuery) setStatisticsResourceType(resourceTypeNames);
    }

    @Override
    public MeanQuery in(TypeName... subTypeNames) {
        return (MeanQuery) super.in(subTypeNames);
    }

    @Override
    public MeanQuery in(Collection<TypeName> subTypeNames) {
        return (MeanQuery) super.in(subTypeNames);
    }

    @Override
    public MeanQuery withGraph(GraknGraph graph) {
        return (MeanQuery) super.withGraph(graph);
    }

    @Override
    String getName() {
        return "mean";
    }
}
