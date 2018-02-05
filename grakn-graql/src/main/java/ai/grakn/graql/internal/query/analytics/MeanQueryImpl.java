/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

import ai.grakn.GraknComputer;
import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.LabelId;
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.MeanMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class MeanQueryImpl extends AbstractStatisticsQuery<Optional<Double>, MeanQuery> implements MeanQuery {

    MeanQueryImpl(Optional<GraknTx> tx) {
        super(tx);
    }

    @Override
    protected final Optional<Double> innerExecute(GraknTx tx, GraknComputer computer) {
        AttributeType.DataType dataType = getDataTypeOfSelectedResourceTypes(tx);
        if (!selectedResourceTypesHaveInstance(tx, statisticsResourceLabels())) return Optional.empty();
        Set<LabelId> allSubLabelIds = convertLabelsToIds(tx, getCombinedSubTypes(tx));
        Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tx, statisticsResourceLabels());

        ComputerResult result = computer.compute(
                new DegreeStatisticsVertexProgram(statisticsResourceLabelIds),
                new MeanMapReduce(statisticsResourceLabelIds, dataType,
                        DegreeVertexProgram.DEGREE),
                allSubLabelIds);
        Map<Serializable, Map<String, Double>> mean = result.memory().get(MeanMapReduce.class.getName());
        Map<String, Double> meanPair = mean.get(MapReduce.NullObject.instance());

        double finalResult = meanPair.get(MeanMapReduce.SUM) / meanPair.get(MeanMapReduce.COUNT);
        LOGGER.debug("Mean = " + finalResult);

        return Optional.of(finalResult);
    }

    @Override
    String getName() {
        return "mean";
    }
}
