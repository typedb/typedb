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
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.graql.analytics.ClusterQuery;
import ai.grakn.graql.internal.analytics.ClusterMemberMapReduce;
import ai.grakn.graql.internal.analytics.ClusterSizeMapReduce;
import ai.grakn.graql.internal.analytics.ConnectedComponentVertexProgram;
import ai.grakn.graql.internal.analytics.ConnectedComponentsVertexProgram;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import ai.grakn.graql.internal.analytics.GraknVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.Memory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class ClusterQueryImpl<T> extends AbstractComputeQuery<T> implements ClusterQuery<T> {

    private boolean members = false;
    private boolean anySize = true;
    private ConceptId sourceId = null;
    private long clusterSize = -1L;

    ClusterQueryImpl(Optional<GraknTx> graph) {
        this.tx = graph;
    }

    @Override
    public T execute() {
        LOGGER.info("ConnectedComponentsVertexProgram is called");
        long startTime = System.currentTimeMillis();
        initSubGraph();
        getAllSubTypes();

        if (!selectedTypesHaveInstance()) {
            LOGGER.info("Selected types don't have instances");
            return (T) Collections.emptyMap();
        }

        Set<LabelId> subLabelIds = convertLabelsToIds(subLabels);

        GraknVertexProgram vertexProgram = sourceId == null ?
                new ConnectedComponentsVertexProgram() :
                new ConnectedComponentVertexProgram(sourceId);
        GraknMapReduce mapReduce;
        if (members) {
            if (anySize) {
                mapReduce = new ClusterMemberMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL);
            } else {
                mapReduce = new ClusterMemberMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL, clusterSize);
            }
        } else {
            if (anySize) {
                mapReduce = new ClusterSizeMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL);
            } else {
                mapReduce = new ClusterSizeMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL, clusterSize);
            }
        }

        Memory memory = getGraphComputer().compute(vertexProgram, mapReduce, subLabelIds).memory();
        LOGGER.info("ConnectedComponentsVertexProgram is done in "
                + (System.currentTimeMillis() - startTime) + " ms");
        return memory.get(members ? ClusterMemberMapReduce.class.getName() : ClusterSizeMapReduce.class.getName());
    }

    @Override
    public ClusterQuery<T> includeAttribute() {
        return (ClusterQuery<T>) super.includeAttribute();
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
    public ClusterQuery<T> clusterSize(ConceptId conceptId) {
        this.sourceId = conceptId;
        return this;
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
    public ClusterQuery<T> in(Collection<Label> subLabels) {
        return (ClusterQuery<T>) super.in(subLabels);
    }

    @Override
    String graqlString() {
        String string = "cluster" + subtypeString();
        if (sourceId != null) {
            string += " from " + sourceId.getValue() + ";";
        }
        if (members) {
            string += " members;";
        }
        if (!anySize) {
            string += " size " + clusterSize + ";";
        }
        return string;
    }

    @Override
    public ClusterQuery<T> withTx(GraknTx tx) {
        return (ClusterQuery<T>) super.withTx(tx);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ClusterQueryImpl<?> that = (ClusterQueryImpl<?>) o;

        return sourceId.equals(that.sourceId) && members == that.members &&
                anySize == that.anySize && clusterSize == that.clusterSize;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (sourceId != null ? sourceId.hashCode() : 0);
        result = 31 * result + (members ? 1 : 0);
        result = 31 * result + (anySize ? 1 : 0);
        result = 31 * result + (int) (clusterSize ^ (clusterSize >>> 32));
        return result;
    }
}
