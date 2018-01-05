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
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.AttributeType;
import ai.grakn.graql.analytics.MinQuery;
import ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.MinMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class MinQueryImpl extends AbstractStatisticsQuery<Optional<Number>> implements MinQuery {

    MinQueryImpl(Optional<GraknTx> graph) {
        this.tx = graph;
    }

    @Override
    public Optional<Number> execute() {
        LOGGER.info("MinMapReduce is called");
        long startTime = System.currentTimeMillis();

        initSubGraph();
        getAllSubTypes();

        AttributeType.DataType dataType = getDataTypeOfSelectedResourceTypes();
        if (!selectedResourceTypesHaveInstance(statisticsResourceLabels)) return Optional.empty();
        Set<LabelId> allSubLabelIds = convertLabelsToIds(getCombinedSubTypes());
        Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(statisticsResourceLabels);

        ComputerResult result = getGraphComputer().compute(
                new DegreeStatisticsVertexProgram(statisticsResourceLabelIds),
                new MinMapReduce(statisticsResourceLabelIds, dataType,
                        DegreeVertexProgram.DEGREE),
                allSubLabelIds);
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
    public MinQuery of(Collection<Label> resourceLabels) {
        return (MinQuery) setStatisticsResourceType(resourceLabels);
    }

    @Override
    public MinQuery in(String... subTypeLabels) {
        return (MinQuery) super.in(subTypeLabels);
    }

    @Override
    public MinQuery in(Collection<Label> subLabels) {
        return (MinQuery) super.in(subLabels);
    }

    @Override
    public MinQuery withTx(GraknTx tx) {
        return (MinQuery) super.withTx(tx);
    }

    @Override
    String getName() {
        return "min";
    }
}
