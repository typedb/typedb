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
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram;
import ai.grakn.graql.internal.analytics.MeanMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;

import java.io.Serializable;
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
        long startTime = System.currentTimeMillis();

        initSubGraph();
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeLabels);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeLabels)) return Optional.empty();
        Set<TypeLabel> allSubTypes = getCombinedSubTypes();

        ComputerResult result = getGraphComputer().compute(
                new DegreeStatisticsVertexProgram(allSubTypes, statisticsResourceTypeLabels),
                new MeanMapReduce(statisticsResourceTypeLabels, dataType));
        Map<Serializable, Map<String, Double>> mean = result.memory().get(MeanMapReduce.class.getName());
        Map<String, Double> meanPair = mean.get(MapReduce.NullObject.instance());

        double finalResult = meanPair.get(MeanMapReduce.SUM) / meanPair.get(MeanMapReduce.COUNT);
        LOGGER.debug("Mean = " + finalResult);

        LOGGER.info("MeanMapReduce is done in " + (System.currentTimeMillis() - startTime) + " ms");
        return Optional.of(finalResult);
    }

    @Override
    public MeanQuery of(String... resourceTypeLabels) {
        return (MeanQuery) setStatisticsResourceType(resourceTypeLabels);
    }

    @Override
    public MeanQuery of(Collection<TypeLabel> resourceTypeLabels) {
        return (MeanQuery) setStatisticsResourceType(resourceTypeLabels);
    }

    @Override
    public MeanQuery in(String... subTypeLabels) {
        return (MeanQuery) super.in(subTypeLabels);
    }

    @Override
    public MeanQuery in(Collection<TypeLabel> subTypeLabels) {
        return (MeanQuery) super.in(subTypeLabels);
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
