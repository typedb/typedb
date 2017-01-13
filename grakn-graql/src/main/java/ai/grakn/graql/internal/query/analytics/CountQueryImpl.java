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
import ai.grakn.concept.TypeName;
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.internal.analytics.CountMapReduce;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

class CountQueryImpl extends AbstractComputeQuery<Long> implements CountQuery {

    CountQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public Long execute() {
        LOGGER.info("CountMapReduce is called");
        initSubGraph();
        if (!selectedTypesHaveInstance()) return 0L;

        ComputerResult result = getGraphComputer().compute(new CountMapReduce(subTypeNames));
        Map<String, Long> count = result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
        LOGGER.info("CountMapReduce is done");
        return count.getOrDefault(CountMapReduce.MEMORY_KEY, 0L);
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public CountQuery in(TypeName... subTypeNames) {
        return (CountQuery) super.in(subTypeNames);
    }

    @Override
    public CountQuery in(Collection<TypeName> subTypeNames) {
        return (CountQuery) super.in(subTypeNames);
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
