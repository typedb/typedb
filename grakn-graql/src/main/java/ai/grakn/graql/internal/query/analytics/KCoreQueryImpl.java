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
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.analytics.KCoreQuery;
import ai.grakn.graql.internal.analytics.ClusterSizeMapReduce;
import ai.grakn.graql.internal.analytics.ConnectedComponentVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeDistributionMapReduce;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class KCoreQueryImpl extends AbstractComputeQuery<Map<String, Set<String>>> implements KCoreQuery {

    private int k = -1;

    KCoreQueryImpl(Optional<GraknTx> graph) {
        this.tx = graph;
    }

    @Override
    public Map<String, Set<String>> execute() {
        LOGGER.info("KCore is called");
        long startTime = System.currentTimeMillis();

        if (k < 2) throw GraqlQueryException.kValueSmallerThanTwo();

        initSubGraph();
        getAllSubTypes();

        if (!selectedTypesHaveInstance()) return Collections.emptyMap();

        Set<LabelId> subLabelIds = convertLabelsToIds(subLabels);

        //TODO
        ComputerResult result = getGraphComputer().compute(
                new DegreeVertexProgram(subLabelIds),
                new ClusterSizeMapReduce(ConnectedComponentVertexProgram.CLUSTER_LABEL),
                subLabelIds);

        LOGGER.info("KCore is done in " + (System.currentTimeMillis() - startTime) + " ms");
        return result.memory().get(DegreeDistributionMapReduce.class.getName());
    }

    @Override
    public KCoreQuery kValue(int kValue) {
        k = kValue;
        return this;
    }

    @Override
    public KCoreQuery in(String... subTypeLabels) {
        return (KCoreQuery) super.in(subTypeLabels);
    }

    @Override
    public KCoreQuery in(Collection<Label> subLabels) {
        return (KCoreQuery) super.in(subLabels);
    }

    //TODO
    @Override
    String graqlString() {
        String string = "kcore";
        string += subtypeString();
        return string;
    }

    @Override
    public KCoreQuery withTx(GraknTx tx) {
        return (KCoreQuery) super.withTx(tx);
    }

    @Override
    public KCoreQuery includeAttribute() {
        return (KCoreQuery) super.includeAttribute();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        KCoreQueryImpl that = (KCoreQueryImpl) o;

        return k == that.k;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + k;
        return result;
    }
}
