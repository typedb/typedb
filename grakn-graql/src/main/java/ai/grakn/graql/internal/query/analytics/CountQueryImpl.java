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
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.internal.analytics.CountMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;

import java.io.Serializable;
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
        long startTime = System.currentTimeMillis();

        initSubGraph();
        if (!selectedTypesHaveInstance()) return 0L;

        ComputerResult result = getGraphComputer().compute(new CountMapReduce(subTypeLabels));
        Map<Serializable, Long> count = result.memory().get(CountMapReduce.class.getName());

        LOGGER.debug("Count = " + count.get(MapReduce.NullObject.instance()));
        LOGGER.info("CountMapReduce is done in " + (System.currentTimeMillis() - startTime) + " ms");
        return count.get(MapReduce.NullObject.instance());
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
    public CountQuery in(Collection<TypeLabel> subTypeLabels) {
        return (CountQuery) super.in(subTypeLabels);
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
