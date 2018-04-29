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
import ai.grakn.graql.analytics.StatisticsQuery;
import ai.grakn.graql.analytics.StdQuery;
import ai.grakn.graql.analytics.SumQuery;
import ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.MaxMapReduce;
import ai.grakn.graql.internal.analytics.MeanMapReduce;
import ai.grakn.graql.internal.analytics.MedianVertexProgram;
import ai.grakn.graql.internal.analytics.MinMapReduce;
import ai.grakn.graql.internal.analytics.StatisticsMapReduce;
import ai.grakn.graql.internal.analytics.StdMapReduce;
import ai.grakn.graql.internal.analytics.SumMapReduce;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import jline.internal.Nullable;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
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
 * @author Haikal Pribadi
 * @author Ganeshwara Herawan Hananda
 */
class TinkerComputeJob<T> implements ComputeJob<T> {

    private final Supplier<T> supplier;
    private final ComputeQuery<?> query;

    private static final Logger LOG = LoggerFactory.getLogger(TinkerComputeQueryExecutor.class);
    private final EmbeddedGraknTx<?> tx;

    TinkerComputeJob(EmbeddedGraknTx<?> tx, Function<GraknComputer, T> function) {
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
        if (supplier != null) return supplier.get();

        if (query instanceof MedianQuery) return (T) runComputeMedian();
        if (query instanceof StdQuery) return (T) runComputeStd();
        if (query instanceof SumQuery) return (T) runComputeSum();
        if (query instanceof MinQuery) return (T) runComputeMin();
        if (query instanceof MeanQuery) return (T) runComputeMean();
        if (query instanceof MaxQuery) return (T) runComputeMax();

        throw GraqlQueryException.invalidComputeMethod();
    }

    private Optional<Number> runComputeMedian() {
        return Optional.ofNullable(runComputeStatistics((MedianQuery) query));
    }

    private Optional<Number> runComputeMin() {
        return Optional.ofNullable(runComputeStatistics((MinQuery) query));
    }

    private Optional<Number> runComputeMax() {
        return Optional.ofNullable(runComputeStatistics((MaxQuery) query));
    }

    private Optional<Double> runComputeMean() {
        Map<String, Double> meanPair = runComputeStatistics((MeanQuery) query);
        if (meanPair == null) return Optional.empty();

        Double mean = meanPair.get(MeanMapReduce.SUM) / meanPair.get(MeanMapReduce.COUNT);

        return Optional.of(mean);
    }

    private Optional<Double> runComputeStd() {
        Map<String, Double> stdTuple = runComputeStatistics((StdQuery) query);
        if (stdTuple == null) return Optional.empty();

        double squareSum = stdTuple.get(StdMapReduce.SQUARE_SUM);
        double sum = stdTuple.get(StdMapReduce.SUM);
        double count = stdTuple.get(StdMapReduce.COUNT);
        Double std = Math.sqrt(squareSum / count - (sum / count) * (sum / count));

        return Optional.of(std);
    }

    private Optional<Number> runComputeSum() {
        return Optional.ofNullable(runComputeStatistics((SumQuery) query));
    }

    @Nullable
    private <T> T runComputeStatistics(StatisticsQuery<?> query) {
        TinkerStatisticsQuery tinkerComputeQuery = TinkerStatisticsQuery.create(tx, query, tx.session().getGraphComputer());
        AttributeType.DataType<?> dataType = tinkerComputeQuery.validateAndGetDataTypes();
        if (!tinkerComputeQuery.ofTypesHaveInstances()) return null;

        Set<LabelId> allTypes = convertLabelsToIds(tinkerComputeQuery.combinedTypes());
        Set<LabelId> ofTypes = convertLabelsToIds(tinkerComputeQuery.ofTypes());

        VertexProgram program = initialiseVertexProgram(query, ofTypes, dataType);
        StatisticsMapReduce<?> mapReduce = initialiseStatisticsMapReduce(query, ofTypes, dataType);
        ComputerResult computerResult = tinkerComputeQuery.compute(program, mapReduce, allTypes);

        if (query instanceof MedianQuery) {
            Number result = computerResult.memory().get(MedianVertexProgram.MEDIAN);
            LOG.debug("Median = " + result);
            return (T) result;
        }

        Map<Serializable, T> resultMap = computerResult.memory().get(mapReduce.getClass().getName());
        LOG.debug("Result = " + resultMap.get(MapReduce.NullObject.instance()));
        return resultMap.get(MapReduce.NullObject.instance());
    }

    private VertexProgram initialiseVertexProgram(StatisticsQuery<?> query, Set<LabelId> ofTypes, AttributeType.DataType<?> dataTypes) {
        if (query instanceof MedianQuery) return new MedianVertexProgram(ofTypes, dataTypes);
        else return new DegreeStatisticsVertexProgram(ofTypes);
    }

    private StatisticsMapReduce<?> initialiseStatisticsMapReduce(StatisticsQuery<?> query,
                                                                 Set<LabelId> ofTypes,
                                                                 AttributeType.DataType<?> dataTypes) {
        if (query instanceof MinQuery) return new MinMapReduce(ofTypes, dataTypes, DegreeVertexProgram.DEGREE);
        if (query instanceof MaxQuery) return new MaxMapReduce(ofTypes, dataTypes, DegreeVertexProgram.DEGREE);
        if (query instanceof MeanQuery) return new MeanMapReduce(ofTypes, dataTypes, DegreeVertexProgram.DEGREE);
        if (query instanceof StdQuery) return new StdMapReduce(ofTypes, dataTypes, DegreeVertexProgram.DEGREE);
        if (query instanceof SumQuery) return new SumMapReduce(ofTypes, dataTypes, DegreeVertexProgram.DEGREE);

        return null;
    }

    private Set<LabelId> convertLabelsToIds(Set<Label> labelSet) {
        return labelSet.stream()
                .map(tx::convertToId)
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }

}
