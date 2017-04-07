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
import ai.grakn.graql.analytics.StdQuery;
import ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram;
import ai.grakn.graql.internal.analytics.StdMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class StdQueryImpl extends AbstractStatisticsQuery<Optional<Double>> implements StdQuery {

    StdQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public Optional<Double> execute() {
        LOGGER.info("StdMapReduce is called");
        long startTime = System.currentTimeMillis();

        initSubGraph();
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeLabels);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeLabels)) return Optional.empty();
        Set<TypeLabel> allSubTypes = getCombinedSubTypes();

        ComputerResult result = getGraphComputer().compute(
                new DegreeStatisticsVertexProgram(allSubTypes, statisticsResourceTypeLabels),
                new StdMapReduce(statisticsResourceTypeLabels, dataType));
        Map<Serializable, Map<String, Double>> std = result.memory().get(StdMapReduce.class.getName());
        Map<String, Double> stdTuple = std.get(MapReduce.NullObject.instance());
        double squareSum = stdTuple.get(StdMapReduce.SQUARE_SUM);
        double sum = stdTuple.get(StdMapReduce.SUM);
        double count = stdTuple.get(StdMapReduce.COUNT);

        double finalResult = Math.sqrt(squareSum / count - (sum / count) * (sum / count));
        LOGGER.debug("Std = " + finalResult);

        LOGGER.info("StdMapReduce is done in " + (System.currentTimeMillis() - startTime) + " ms");
        return Optional.of(finalResult);
    }

    @Override
    public StdQuery of(String... resourceTypeLabels) {
        return (StdQuery) setStatisticsResourceType(resourceTypeLabels);
    }

    @Override
    public StdQuery of(Collection<TypeLabel> resourceTypeLabels) {
        return (StdQuery) setStatisticsResourceType(resourceTypeLabels);
    }

    @Override
    public StdQuery in(String... subTypeLabels) {
        return (StdQuery) super.in(subTypeLabels);
    }

    @Override
    public StdQuery in(Collection<TypeLabel> subTypeLabels) {
        return (StdQuery) super.in(subTypeLabels);
    }

    @Override
    public StdQuery withGraph(GraknGraph graph) {
        return (StdQuery) super.withGraph(graph);
    }

    @Override
    String getName() {
        return "std";
    }
}
