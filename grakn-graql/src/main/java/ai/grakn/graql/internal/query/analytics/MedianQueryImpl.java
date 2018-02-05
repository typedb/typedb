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

import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.LabelId;
import ai.grakn.graql.analytics.MedianQuery;
import ai.grakn.graql.internal.analytics.MedianVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Optional;
import java.util.Set;

class MedianQueryImpl extends AbstractStatisticsQuery<Optional<Number>, MedianQuery> implements MedianQuery {

    MedianQueryImpl(Optional<GraknTx> tx) {
        super(tx);
    }

    @Override
    protected final Optional<Number> innerExecute(GraknTx tx) {
        AttributeType.DataType dataType = getDataTypeOfSelectedResourceTypes();
        if (!selectedResourceTypesHaveInstance(tx, statisticsResourceLabels())) return Optional.empty();
        Set<LabelId> allSubLabelIds = convertLabelsToIds(tx, getCombinedSubTypes());
        Set<LabelId> statisticsResourceLabelIds = convertLabelsToIds(tx, statisticsResourceLabels());

        ComputerResult result = getGraphComputer().compute(
                new MedianVertexProgram(statisticsResourceLabelIds, dataType),
                null, allSubLabelIds);

        Number finalResult = result.memory().get(MedianVertexProgram.MEDIAN);
        LOGGER.debug("Median = " + finalResult);

        return Optional.of(finalResult);
    }

    @Override
    String getName() {
        return "median";
    }
}
