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
import ai.grakn.concept.Concept;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.internal.analytics.CountMapReduceWithAttribute;
import ai.grakn.graql.internal.analytics.CountVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.analytics.GraknMapReduce.RESERVED_TYPE_LABEL_KEY;

class CountQueryImpl extends AbstractComputeQuery<Long, CountQuery> implements CountQuery {

    CountQueryImpl(Optional<GraknTx> tx) {
        super(tx);
    }

    @Override
    protected final Long innerExecute(GraknTx tx, GraknComputer computer) {
        if (!selectedTypesHaveInstance(tx)) {
            LOGGER.debug("Count = 0");
            return 0L;
        }

        Set<LabelId> typeLabelIds = convertLabelsToIds(tx, subLabels(tx));
        Map<Integer, Long> count;

        Set<LabelId> rolePlayerLabelIds = getRolePlayerLabelIds(tx);
        rolePlayerLabelIds.addAll(typeLabelIds);

        ComputerResult result = computer.compute(
                new CountVertexProgram(),
                new CountMapReduceWithAttribute(),
                rolePlayerLabelIds, false);
        count = result.memory().get(CountMapReduceWithAttribute.class.getName());

        long finalCount = count.keySet().stream()
                .filter(id -> typeLabelIds.contains(LabelId.of(id)))
                .mapToLong(count::get).sum();
        if (count.containsKey(RESERVED_TYPE_LABEL_KEY)) {
            finalCount += count.get(RESERVED_TYPE_LABEL_KEY);
        }

        LOGGER.debug("Count = " + finalCount);
        return finalCount;
    }

    @Override
    String graqlString() {
        return "count" + subtypeString();
    }

    private Set<LabelId> getRolePlayerLabelIds(GraknTx tx) {
        return subTypes(tx)
                .filter(Concept::isRelationshipType)
                .map(Concept::asRelationshipType)
                .filter(RelationshipType::isImplicit)
                .flatMap(RelationshipType::relates)
                .flatMap(Role::playedByTypes)
                .map(type -> tx.admin().convertToId(type.getLabel()))
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }
}
