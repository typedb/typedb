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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query.runner;

import ai.grakn.ComputeJob;
import ai.grakn.GraknComputer;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.analytics.ComputeQuery;
import ai.grakn.graql.analytics.MaxQuery;
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.analytics.MedianQuery;
import ai.grakn.graql.analytics.MinQuery;
import ai.grakn.graql.analytics.StdQuery;
import ai.grakn.graql.analytics.SumQuery;
import ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.MaxMapReduce;
import ai.grakn.graql.internal.analytics.MeanMapReduce;
import ai.grakn.graql.internal.analytics.MedianVertexProgram;
import ai.grakn.graql.internal.analytics.MinMapReduce;
import ai.grakn.graql.internal.analytics.StdMapReduce;
import ai.grakn.graql.internal.analytics.SumMapReduce;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A compute query job executed against a {@link GraknComputer}.
 *
 * @author Felix Chapman
 */
class TinkerComputeJob<T> implements ComputeJob<T> {

    private final Supplier<T> supplier;
    private final ComputeQuery<?> query;

    private static final Logger LOG = LoggerFactory.getLogger(TinkerComputeQueryExecutor.class);
    private final EmbeddedGraknTx<?> tx;

    public TinkerComputeJob (EmbeddedGraknTx<?> tx, Function<GraknComputer, T> function) {
        this.tx = tx;
        this.supplier = () -> function.apply(this.tx.session().getGraphComputer());

        this.query = null; //todo: to be removed;
    }

    public TinkerComputeJob(EmbeddedGraknTx<?> tx, ComputeQuery<?> query) {
        this.tx = tx;
        this.query = query;

        this.supplier = null; //todo: to be removed;
    }

    @Override
    public void kill() { //todo: to be removed;
        tx.session().getGraphComputer().killJobs();
    }

    @Override
    public T get() {
        if(supplier != null) return supplier.get();

        if (query instanceof MedianQuery) return (T) runComputeMedian();
        if (query instanceof StdQuery) return (T) runComputeStd();
        if (query instanceof SumQuery) return (T) runComputeSum();
        if (query instanceof MinQuery) return (T) runComputeMin();
        if (query instanceof MeanQuery) return (T) runComputeMean();
        if (query instanceof MaxQuery) return (T) runComputeMax();

        throw GraqlQueryException.invalidComputeMethod();
    }

    public Optional<Number> runComputeMedian() {
        TinkerStatisticsQuery tinkerComputeQuery = TinkerStatisticsQuery.create(tx, (MedianQuery) query, tx.session().getGraphComputer());
        AttributeType.DataType<?> dataType = tinkerComputeQuery.getDataTypeOfSelectedAttributeTypes();
        if (!tinkerComputeQuery.selectedAttributeTypesHaveInstance()) {
            return Optional.empty();
        }
        Set<LabelId> allSubLabelIds = convertLabelsToIds(tinkerComputeQuery.getCombinedSubTypes());
        Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tinkerComputeQuery.statisticsAttributeLabels());

        ComputerResult result = tinkerComputeQuery.compute(
                new MedianVertexProgram(statisticsResourceLabelIds, dataType),
                null, allSubLabelIds);

        Number finalResult = result.memory().get(MedianVertexProgram.MEDIAN);
        LOG.debug("Median = " + finalResult);

        return Optional.of(finalResult);
    }

    public Optional<Double> runComputeStd() {
        TinkerStatisticsQuery tinkerComputeQuery = TinkerStatisticsQuery.create(tx, (StdQuery) query, tx.session().getGraphComputer());
        AttributeType.DataType<?> dataType = tinkerComputeQuery.getDataTypeOfSelectedAttributeTypes();
        if (!tinkerComputeQuery.selectedAttributeTypesHaveInstance()) {
            return Optional.empty();
        }
        Set<LabelId> allSubLabelIds = convertLabelsToIds(tinkerComputeQuery.getCombinedSubTypes());
        Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tinkerComputeQuery.statisticsAttributeLabels());

        StdMapReduce mapReduce = new StdMapReduce(statisticsResourceLabelIds, dataType, DegreeVertexProgram.DEGREE);

        ComputerResult result = tinkerComputeQuery.compute(
                new DegreeStatisticsVertexProgram(statisticsResourceLabelIds),
                mapReduce,
                allSubLabelIds);
        Map<Serializable, Map<String, Double>> map = result.memory().get(mapReduce.getClass().getName());

        LOG.debug("Result = " + map.get(MapReduce.NullObject.instance()));

        Map<String, Double> stdTuple = map.get(MapReduce.NullObject.instance());
        double squareSum = stdTuple.get(StdMapReduce.SQUARE_SUM);
        double sum = stdTuple.get(StdMapReduce.SUM);
        double count = stdTuple.get(StdMapReduce.COUNT);
        Double std = Math.sqrt(squareSum / count - (sum / count) * (sum / count));
        return Optional.of(std);
    }

    public Optional<Number> runComputeSum() {
        TinkerStatisticsQuery tinkerComputeQuery = TinkerStatisticsQuery.create(tx, (SumQuery) query, tx.session().getGraphComputer());
        AttributeType.DataType<?> dataType = tinkerComputeQuery.getDataTypeOfSelectedAttributeTypes();
        if (!tinkerComputeQuery.selectedAttributeTypesHaveInstance()) {
            return Optional.empty();
        }
        Set<LabelId> allSubLabelIds = convertLabelsToIds(tinkerComputeQuery.getCombinedSubTypes());
        Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tinkerComputeQuery.statisticsAttributeLabels());

        SumMapReduce mapReduce = new SumMapReduce(statisticsResourceLabelIds, dataType, DegreeVertexProgram.DEGREE);

        ComputerResult result = tinkerComputeQuery.compute(
                new DegreeStatisticsVertexProgram(statisticsResourceLabelIds),
                mapReduce,
                allSubLabelIds);
        Map<Serializable, Number> map = result.memory().get(mapReduce.getClass().getName());

        LOG.debug("Result = " + map.get(MapReduce.NullObject.instance()));

        Number sum = map.get(MapReduce.NullObject.instance());
        return Optional.of(sum);
    }

    public Optional<Number> runComputeMin() {
        TinkerStatisticsQuery tinkerComputeQuery = TinkerStatisticsQuery.create(tx, (MinQuery) query, tx.session().getGraphComputer());
        AttributeType.DataType<?> dataType = tinkerComputeQuery.getDataTypeOfSelectedAttributeTypes();
        if (!tinkerComputeQuery.selectedAttributeTypesHaveInstance()) {
            return Optional.empty();
        }
        Set<LabelId> allSubLabelIds = convertLabelsToIds(tinkerComputeQuery.getCombinedSubTypes());
        Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tinkerComputeQuery.statisticsAttributeLabels());

        MinMapReduce mapReduce = new MinMapReduce(statisticsResourceLabelIds, dataType, DegreeVertexProgram.DEGREE);

        ComputerResult result = tinkerComputeQuery.compute(
                new DegreeStatisticsVertexProgram(statisticsResourceLabelIds),
                mapReduce,
                allSubLabelIds);
        Map<Serializable, Number> map = result.memory().get(mapReduce.getClass().getName());

        LOG.debug("Result = " + map.get(MapReduce.NullObject.instance()));

        Number min = map.get(MapReduce.NullObject.instance());
        return Optional.of(min);
    }

    public Optional<Double> runComputeMean() {
        TinkerStatisticsQuery tinkerComputeQuery = TinkerStatisticsQuery.create(tx, (MeanQuery) query, tx.session().getGraphComputer());
        AttributeType.DataType<?> dataType = tinkerComputeQuery.getDataTypeOfSelectedAttributeTypes();
        if (!tinkerComputeQuery.selectedAttributeTypesHaveInstance()) {
            return Optional.empty();
        }
        Set<LabelId> allSubLabelIds = convertLabelsToIds(tinkerComputeQuery.getCombinedSubTypes());
        Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tinkerComputeQuery.statisticsAttributeLabels());

        MeanMapReduce mapReduce = new MeanMapReduce(statisticsResourceLabelIds, dataType, DegreeVertexProgram.DEGREE);

        ComputerResult result = tinkerComputeQuery.compute(
                new DegreeStatisticsVertexProgram(statisticsResourceLabelIds),
                mapReduce,
                allSubLabelIds);
        Map<Serializable, Map<String, Double>> map = result.memory().get(mapReduce.getClass().getName());

        LOG.debug("Result = " + map.get(MapReduce.NullObject.instance()));

        Map<String, Double> meanPair = map.get(MapReduce.NullObject.instance());
        Double mean = meanPair.get(MeanMapReduce.SUM) / meanPair.get(MeanMapReduce.COUNT);
        return Optional.of(mean);
    }

    public Optional<Number> runComputeMax() {
        TinkerStatisticsQuery tinkerComputeQuery = TinkerStatisticsQuery.create(tx, (MaxQuery) query, tx.session().getGraphComputer());
        AttributeType.DataType<?> dataType = tinkerComputeQuery.getDataTypeOfSelectedAttributeTypes();
        if (!tinkerComputeQuery.selectedAttributeTypesHaveInstance()) {
            return Optional.empty();
        }
        Set<LabelId> allSubLabelIds = convertLabelsToIds(tinkerComputeQuery.getCombinedSubTypes());
        Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tinkerComputeQuery.statisticsAttributeLabels());

        MaxMapReduce mapReduce = new MaxMapReduce(statisticsResourceLabelIds, dataType, DegreeVertexProgram.DEGREE);

        ComputerResult result = tinkerComputeQuery.compute(
                new DegreeStatisticsVertexProgram(statisticsResourceLabelIds),
                mapReduce,
                allSubLabelIds);
        Map<Serializable, Number> map = result.memory().get(mapReduce.getClass().getName());

        LOG.debug("Result = " + map.get(MapReduce.NullObject.instance()));

        Number max = map.get(MapReduce.NullObject.instance());
        return Optional.of(max);
    }

    private Set<LabelId> convertLabelsToIds(Set<Label> labelSet) {
        return labelSet.stream()
                .map(tx::convertToId)
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }

}
