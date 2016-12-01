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

import ai.grakn.GraknComputer;
import ai.grakn.GraknGraph;
import ai.grakn.concept.ResourceType;
import ai.grakn.graql.analytics.ClusterQuery;
import ai.grakn.graql.internal.analytics.ClusterMemberMapReduce;
import ai.grakn.graql.internal.analytics.ClusterSizeMapReduce;
import ai.grakn.graql.internal.analytics.ConnectedComponentVertexProgram;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.graql.internal.analytics.CommonOLAP.analyticsElements;

public class ClusterQueryImpl<T> extends AbstractComputeQuery<T> implements ClusterQuery<T> {

    private boolean members = false;
    private boolean persist = false;

    public ClusterQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public T execute() {
        LOGGER.info("ConnectedComponentsVertexProgram is called");
        initSubGraph();
        if (!selectedTypesHaveInstance()) return (T) Collections.emptyMap();

        ComputerResult result;
        GraknComputer computer = getGraphComputer();

        if (members) {
            if (persist) {
                if (!Sets.intersection(subTypeNames, analyticsElements).isEmpty()) {
                    throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                            .getMessage(this.getClass().toString()));
                }
                mutateResourceOntology(connectedComponent, ResourceType.DataType.STRING);
                waitOnMutateResourceOntology(connectedComponent);
                result = computer.compute(new ConnectedComponentVertexProgram(subTypeNames, keySpace),
                        new ClusterMemberMapReduce(subTypeNames, ConnectedComponentVertexProgram.CLUSTER_LABEL));
            } else {
                result = computer.compute(new ConnectedComponentVertexProgram(subTypeNames),
                        new ClusterMemberMapReduce(subTypeNames, ConnectedComponentVertexProgram.CLUSTER_LABEL));
            }
        } else {
            if (persist) {
                if (!Sets.intersection(subTypeNames, analyticsElements).isEmpty()) {
                    throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                            .getMessage(this.getClass().toString()));
                }
                mutateResourceOntology(connectedComponent, ResourceType.DataType.STRING);
                waitOnMutateResourceOntology(connectedComponent);
                result = computer.compute(new ConnectedComponentVertexProgram(subTypeNames, keySpace),
                        new ClusterSizeMapReduce(subTypeNames, ConnectedComponentVertexProgram.CLUSTER_LABEL));
            } else {
                result = computer.compute(new ConnectedComponentVertexProgram(subTypeNames),
                        new ClusterSizeMapReduce(subTypeNames, ConnectedComponentVertexProgram.CLUSTER_LABEL));
            }
        }
        LOGGER.info("ConnectedComponentsVertexProgram is done");
        return (T) result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public ClusterQuery<Map<String, Set<String>>> members() {
        this.members = true;
        return (ClusterQuery<Map<String, Set<String>>>) this;
    }

    @Override
    public ClusterQuery<T> persist() {
        this.persist = true;
        return this;
    }

    @Override
    public ClusterQuery<T> in(String... subTypeNames) {
        return (ClusterQuery<T>) super.in(subTypeNames);
    }

    @Override
    public ClusterQuery<T> in(Collection<String> subTypeNames) {
        return (ClusterQuery<T>) super.in(subTypeNames);
    }

    @Override
    public ClusterQuery<T> withGraph(GraknGraph graph) {
        return (ClusterQuery<T>) super.withGraph(graph);
    }

}
