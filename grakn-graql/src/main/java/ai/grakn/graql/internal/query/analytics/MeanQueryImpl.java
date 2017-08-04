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
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.ResourceType;
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
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
        ResourceType.DataType dataType = getDataTypeOfSelectedResourceTypes(statisticsResourceLabels);
        if (!selectedResourceTypesHaveInstance(statisticsResourceLabels)) return Optional.empty();
        Set<LabelId> allSubLabelIds = convertLabelsToIds(getCombinedSubTypes());
        Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(statisticsResourceLabels);

        String randomId = getRandomJobId();

        ComputerResult result = getGraphComputer().compute(
                new DegreeStatisticsVertexProgram(statisticsResourceLabelIds, randomId),
                new MeanMapReduce(statisticsResourceLabelIds, dataType,
                        DegreeVertexProgram.DEGREE + randomId),
                allSubLabelIds);
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
    public MeanQuery of(Collection<Label> resourceLabels) {
        return (MeanQuery) setStatisticsResourceType(resourceLabels);
    }

    @Override
    public MeanQuery in(String... subTypeLabels) {
        return (MeanQuery) super.in(subTypeLabels);
    }

    @Override
    public MeanQuery in(Collection<Label> subLabels) {
        return (MeanQuery) super.in(subLabels);
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
