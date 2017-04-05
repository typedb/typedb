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
import ai.grakn.graql.analytics.ClusterQuery;
import ai.grakn.graql.internal.analytics.ClusterMemberMapReduce;
import ai.grakn.graql.internal.analytics.ClusterSizeMapReduce;
import ai.grakn.graql.internal.analytics.ConnectedComponentVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

class ClusterQueryImpl<T> extends AbstractComputeQuery<T> implements ClusterQuery<T> {

    private boolean members = false;
    private boolean anySize = true;
    private long clusterSize = -1L;

    ClusterQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public T execute() {
        LOGGER.info("ConnectedComponentsVertexProgram is called");
        long startTime = System.currentTimeMillis();
        initSubGraph();
        if (!selectedTypesHaveInstance()) return (T) Collections.emptyMap();

        ComputerResult result;
        Set<TypeLabel> withResourceRelationTypes = getHasResourceRelationTypes();
        withResourceRelationTypes.addAll(subTypeLabels);

        String randomId = Integer.toString(ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
        if (members) {
            if (anySize) {
                result = getGraphComputer().compute(
                        new ConnectedComponentVertexProgram(withResourceRelationTypes, randomId),
                        new ClusterMemberMapReduce(subTypeLabels,
                                ConnectedComponentVertexProgram.CLUSTER_LABEL + randomId));
            } else {
                result = getGraphComputer().compute(
                        new ConnectedComponentVertexProgram(withResourceRelationTypes, randomId),
                        new ClusterMemberMapReduce(subTypeLabels,
                                ConnectedComponentVertexProgram.CLUSTER_LABEL + randomId, clusterSize));
            }
            LOGGER.info("ConnectedComponentsVertexProgram is done in "
                    + (System.currentTimeMillis() - startTime) + " ms");
            return result.memory().get(ClusterMemberMapReduce.class.getName());
        } else {
            if (anySize) {
                result = getGraphComputer().compute(
                        new ConnectedComponentVertexProgram(withResourceRelationTypes, randomId),
                        new ClusterSizeMapReduce(subTypeLabels,
                                ConnectedComponentVertexProgram.CLUSTER_LABEL + randomId));
            } else {
                result = getGraphComputer().compute(
                        new ConnectedComponentVertexProgram(withResourceRelationTypes, randomId),
                        new ClusterSizeMapReduce(subTypeLabels,
                                ConnectedComponentVertexProgram.CLUSTER_LABEL + randomId, clusterSize));
            }
            LOGGER.info("ConnectedComponentsVertexProgram is done in "
                    + (System.currentTimeMillis() - startTime) + " ms");
            return result.memory().get(ClusterSizeMapReduce.class.getName());
        }
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public ClusterQuery<Map<String, Set<String>>> members() {
        this.members = true;
        return (ClusterQuery<Map<String, Set<String>>>) this;
    }

    @Override
    public ClusterQuery<T> clusterSize(long clusterSize) {
        this.anySize = false;
        this.clusterSize = clusterSize;
        return this;
    }

    @Override
    public ClusterQuery<T> in(String... subTypeLabels) {
        return (ClusterQuery<T>) super.in(subTypeLabels);
    }

    @Override
    public ClusterQuery<T> in(Collection<TypeLabel> subTypeLabels) {
        return (ClusterQuery<T>) super.in(subTypeLabels);
    }

    @Override
    String graqlString() {
        String string = "cluster" + subtypeString();
        if (members) {
            string += " members;";
        }
        if (!anySize) {
            string += " size " + clusterSize + ";";
        }
        return string;
    }

    @Override
    public ClusterQuery<T> withGraph(GraknGraph graph) {
        return (ClusterQuery<T>) super.withGraph(graph);
    }

}
