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
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.internal.analytics.CountMapReduce;
import ai.grakn.graql.internal.analytics.CountMapReduceWithAttribute;
import ai.grakn.graql.internal.analytics.CountVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.graql.internal.analytics.GraknMapReduce.RESERVED_TYPE_LABEL_KEY;

class CountQueryImpl extends AbstractComputeQuery<Long> implements CountQuery {

    CountQueryImpl(Optional<GraknTx> graph) {
        this.tx = graph;
    }

    @Override
    public Long execute() {
        LOGGER.info("CountMapReduce is called");
        long startTime = System.currentTimeMillis();

        initSubGraph();
        getAllSubTypes();

        if (!selectedTypesHaveInstance()) {
            LOGGER.debug("Count = 0");
            LOGGER.info("CountMapReduce is done in " + (System.currentTimeMillis() - startTime) + " ms");
            return 0L;
        }

        Set<LabelId> typeLabelIds = convertLabelsToIds(subLabels);
        Map<Integer, Long> count;

        if (includeAttribute) {
            Set<LabelId> rolePlayerLabelIds = getRolePlayerLabelIds();
            rolePlayerLabelIds.addAll(typeLabelIds);

            ComputerResult result = getGraphComputer().compute(
                    new CountVertexProgram(),
                    new CountMapReduceWithAttribute(),
                    rolePlayerLabelIds, false);
            count = result.memory().get(CountMapReduceWithAttribute.class.getName());
        } else {
            ComputerResult result = getGraphComputer().compute(
                    null,
                    new CountMapReduce(),
                    typeLabelIds, false);
            count = result.memory().get(CountMapReduce.class.getName());
        }

        long finalCount = count.keySet().stream()
                .filter(id -> typeLabelIds.contains(LabelId.of(id)))
                .mapToLong(count::get).sum();
        if (count.containsKey(RESERVED_TYPE_LABEL_KEY)) {
            finalCount += count.get(RESERVED_TYPE_LABEL_KEY);
        }

        LOGGER.debug("Count = " + finalCount);
        LOGGER.info("CountMapReduce is done in " + (System.currentTimeMillis() - startTime) + " ms");
        return finalCount;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public CountQuery in(String... subTypeLabels) {
        return (CountQuery) super.in(subTypeLabels);
    }

    @Override
    public CountQuery in(Collection<Label> subLabels) {
        return (CountQuery) super.in(subLabels);
    }

    @Override
    String graqlString() {
        return "count" + subtypeString();
    }

    @Override
    public CountQuery withTx(GraknTx tx) {
        return (CountQuery) super.withTx(tx);
    }
}
