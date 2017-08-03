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
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.RelationType;
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.internal.analytics.CountMapReduce;
import ai.grakn.graql.internal.analytics.CountVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.analytics.GraknMapReduce.RESERVED_TYPE_LABEL_KEY;

class CountQueryImpl extends AbstractComputeQuery<Long> implements CountQuery {

    CountQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public Long execute() {
        LOGGER.info("CountMapReduce is called");
        long startTime = System.currentTimeMillis();

        initSubGraph();
        if (!selectedTypesHaveInstance()) {
            LOGGER.debug("Count = 0");
            LOGGER.info("CountMapReduce is done in " + (System.currentTimeMillis() - startTime) + " ms");
            return 0L;
        }

        Set<LabelId> rolePlayerLabelIds = subTypes.stream()
                .filter(Concept::isRelationType)
                .map(relationType -> ((RelationType) relationType).relates())
                .filter(roles -> roles.size() == 2)
                .flatMap(roles -> roles.stream().flatMap(role -> role.playedByTypes().stream()))
                .map(type -> graph.get().admin().convertToId(type.getLabel()))
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());

        Set<LabelId> typeLabelIds = convertLabelsToIds(subLabels);
        rolePlayerLabelIds.addAll(typeLabelIds);

        String randomId = getRandomJobId();

        ComputerResult result = getGraphComputer().compute(
                false, rolePlayerLabelIds,
                new CountVertexProgram(randomId),
                new CountMapReduce(CountVertexProgram.EDGE_COUNT + randomId));

        Map<Integer, Long> count = result.memory().get(CountMapReduce.class.getName());

        long finalCount = count.keySet().stream()
                .filter(id -> typeLabelIds.contains(LabelId.of(id)))
                .map(count::get)
                .reduce(0L, (x, y) -> x + y);
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
    public CountQuery withGraph(GraknGraph graph) {
        return (CountQuery) super.withGraph(graph);
    }

}
