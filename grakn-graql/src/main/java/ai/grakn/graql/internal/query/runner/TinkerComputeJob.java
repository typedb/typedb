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
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.graql.analytics.ComputeQuery;
import ai.grakn.graql.analytics.MedianQuery;
import ai.grakn.graql.internal.analytics.MedianVertexProgram;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static ai.grakn.util.GraqlSyntax.Compute.MEDIAN;

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

        if(query instanceof MedianQuery) return (T) runComputeMedian();

        throw GraqlQueryException.invalidComputeMethod();
    }

    public Optional<Number> runComputeMedian() {
        TinkerStatisticsQuery tinkerComputeQuery = TinkerStatisticsQuery.create(tx, (MedianQuery) query, tx.session().getGraphComputer());
        AttributeType.DataType<?> dataType = tinkerComputeQuery.getDataTypeOfSelectedResourceTypes();
        if (!tinkerComputeQuery.selectedResourceTypesHaveInstance()) {
            return Optional.empty();
        }
        Set<LabelId> allSubLabelIds = convertLabelsToIds(tinkerComputeQuery.getCombinedSubTypes());
        Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tinkerComputeQuery.statisticsResourceLabels());

        ComputerResult result = tinkerComputeQuery.compute(
                new MedianVertexProgram(statisticsResourceLabelIds, dataType),
                null, allSubLabelIds);

        Number finalResult = result.memory().get(MedianVertexProgram.MEDIAN);
        LOG.debug("Median = " + finalResult);

        return Optional.of(finalResult);
    }


    private Set<LabelId> convertLabelsToIds(Set<Label> labelSet) {
        return labelSet.stream()
                .map(tx::convertToId)
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }

}
