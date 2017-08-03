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
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.LabelId;
import ai.grakn.graql.analytics.SumQuery;
import ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.SumMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class SumQueryImpl extends AbstractStatisticsQuery<Optional<Number>> implements SumQuery {

    SumQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public Optional<Number> execute() {
        LOGGER.info("SumMapReduce is called");
        long startTime = System.currentTimeMillis();

        initSubGraph();
        ResourceType.DataType dataType = getDataTypeOfSelectedResourceTypes(statisticsResourceLabels);
        if (!selectedResourceTypesHaveInstance(statisticsResourceLabels)) return Optional.empty();
        Set<LabelId> allSubLabelIds = convertLabelsToIds(getCombinedSubTypes());
        Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(statisticsResourceLabels);

        String randomId = getRandomJobId();

        ComputerResult result = getGraphComputer().compute(allSubLabelIds,
                new DegreeStatisticsVertexProgram(statisticsResourceLabelIds, randomId),
                new SumMapReduce(statisticsResourceLabelIds, dataType, DegreeVertexProgram.DEGREE + randomId));
        Map<Serializable, Number> sum = result.memory().get(SumMapReduce.class.getName());

        Number finalResult = sum.get(MapReduce.NullObject.instance());
        LOGGER.info("Sum = " + finalResult);

        LOGGER.info("SumMapReduce is done in " + (System.currentTimeMillis() - startTime) + " ms");
        return Optional.of(finalResult);
    }

    @Override
    public SumQuery of(String... resourceTypeLabels) {
        return (SumQuery) setStatisticsResourceType(resourceTypeLabels);
    }

    @Override
    public SumQuery of(Collection<Label> resourceLabels) {
        return (SumQuery) setStatisticsResourceType(resourceLabels);
    }

    @Override
    public SumQuery in(String... subTypeLabels) {
        return (SumQuery) super.in(subTypeLabels);
    }

    @Override
    public SumQuery in(Collection<Label> subLabels) {
        return (SumQuery) super.in(subLabels);
    }

    @Override
    public SumQuery withGraph(GraknGraph graph) {
        return (SumQuery) super.withGraph(graph);
    }

    @Override
    String getName() {
        return "sum";
    }
}
