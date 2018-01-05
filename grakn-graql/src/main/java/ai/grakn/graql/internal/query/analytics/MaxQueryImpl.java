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

import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.graql.analytics.MaxQuery;
import ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.MaxMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class MaxQueryImpl extends AbstractStatisticsQuery<Optional<Number>> implements MaxQuery {

    MaxQueryImpl(Optional<GraknTx> graph) {
        this.tx = graph;
    }

    @Override
    public Optional<Number> execute() {
        LOGGER.info("MaxMapReduce is called");
        long startTime = System.currentTimeMillis();

        initSubGraph();
        getAllSubTypes();

        AttributeType.DataType dataType = getDataTypeOfSelectedResourceTypes();
        if (!selectedResourceTypesHaveInstance(statisticsResourceLabels)) return Optional.empty();
        Set<LabelId> allSubLabelIds = convertLabelsToIds(getCombinedSubTypes());
        Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(statisticsResourceLabels);

        ComputerResult result = getGraphComputer().compute(
                new DegreeStatisticsVertexProgram(statisticsResourceLabelIds),
                new MaxMapReduce(statisticsResourceLabelIds, dataType,
                        DegreeVertexProgram.DEGREE),
                allSubLabelIds);
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
    public MaxQuery of(Collection<Label> resourceLabels) {
        return (MaxQuery) setStatisticsResourceType(resourceLabels);
    }

    @Override
    public MaxQuery in(String... subTypeLabels) {
        return (MaxQuery) super.in(subTypeLabels);
    }

    @Override
    public MaxQuery in(Collection<Label> subLabels) {
        return (MaxQuery) super.in(subLabels);
    }

    @Override
    public MaxQuery withTx(GraknTx tx) {
        return (MaxQuery) super.withTx(tx);
    }

    @Override
    String getName() {
        return "max";
    }
}
