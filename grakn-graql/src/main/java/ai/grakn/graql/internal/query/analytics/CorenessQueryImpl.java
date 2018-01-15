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
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.analytics.CorenessQuery;
import ai.grakn.graql.internal.analytics.ClusterMemberMapReduce;
import ai.grakn.graql.internal.analytics.CorenessVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeDistributionMapReduce;
import ai.grakn.graql.internal.analytics.KCoreVertexProgram;
import ai.grakn.graql.internal.analytics.NoResultException;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

class CorenessQueryImpl extends AbstractComputeQuery<Map<Integer, Set<String>>> implements CorenessQuery {

    private int k = 2;
    private boolean ofTypeLabelsSet = false;
    private Set<Label> ofLabels = new HashSet<>();

    CorenessQueryImpl(Optional<GraknTx> graph) {
        this.tx = graph;
    }

    @Override
    public Map<Integer, Set<String>> execute() {
        LOGGER.info("Coreness query is started");
        long startTime = System.currentTimeMillis();

        if (k < 2) throw GraqlQueryException.kValueSmallerThanTwo();

        includeAttribute = true; //TODO: REMOVE THIS LINE
        initSubGraph();
        getAllSubTypes();

        // Check if ofType is valid before returning emptyMap
        if (ofLabels.isEmpty()) {
            ofLabels.addAll(subLabels);
        } else {
            ofLabels = ofLabels.stream()
                    .flatMap(typeLabel -> {
                        Type type = tx.get().getSchemaConcept(typeLabel);
                        if (type == null) throw GraqlQueryException.labelNotFound(typeLabel);
                        if (type.isRelationshipType()) throw GraqlQueryException.kCoreOnRelationshipType(typeLabel);
                        return type.subs();
                    })
                    .map(SchemaConcept::getLabel)
                    .collect(Collectors.toSet());
            subLabels.addAll(ofLabels);
        }

        if (!selectedTypesHaveInstance()) {
            LOGGER.info("Coreness query is finished in " + (System.currentTimeMillis() - startTime) + " ms");
            return Collections.emptyMap();
        }

        ComputerResult result;
        Set<LabelId> subLabelIds = convertLabelsToIds(subLabels);
        Set<LabelId> ofLabelIds = convertLabelsToIds(ofLabels);

        try {
            result = getGraphComputer().compute(
                    new CorenessVertexProgram(k),
                    new DegreeDistributionMapReduce(ofLabelIds, KCoreVertexProgram.CLUSTER_LABEL),
                    subLabelIds);
        } catch (NoResultException e) {
            LOGGER.info("Coreness query is finished in " + (System.currentTimeMillis() - startTime) + " ms");
            return Collections.emptyMap();
        }

        LOGGER.info("Coreness query is finished in " + (System.currentTimeMillis() - startTime) + " ms");
        return result.memory().get(ClusterMemberMapReduce.class.getName());
    }

    @Override
    public CorenessQuery minK(int k) {
        this.k = k;
        return this;
    }

    @Override
    public CorenessQuery of(String... ofTypeLabels) {
        if (ofTypeLabels.length > 0) {
            ofTypeLabelsSet = true;
            this.ofLabels = Arrays.stream(ofTypeLabels).map(Label::of).collect(Collectors.toSet());
        }
        return this;
    }

    @Override
    public CorenessQuery of(Collection<Label> ofLabels) {
        if (!ofLabels.isEmpty()) {
            ofTypeLabelsSet = true;
            this.ofLabels = Sets.newHashSet(ofLabels);
        }
        return this;
    }

    @Override
    public CorenessQuery in(String... subTypeLabels) {
        return (CorenessQuery) super.in(subTypeLabels);
    }

    @Override
    public CorenessQuery in(Collection<Label> subLabels) {
        return (CorenessQuery) super.in(subLabels);
    }

    @Override
    String graqlString() {
        String string = "centrality";
        if (ofTypeLabelsSet) {
            string += " of " + ofLabels.stream()
                    .map(StringConverter::typeLabelToString)
                    .collect(joining(", "));
        }
        string += subtypeString();
        string += " using k-core where k = ";
        string += k + ";";

        return string;
    }

    @Override
    public CorenessQuery withTx(GraknTx tx) {
        return (CorenessQuery) super.withTx(tx);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        CorenessQueryImpl that = (CorenessQueryImpl) o;

        return k == that.k && ofTypeLabelsSet == that.ofTypeLabelsSet && ofLabels.equals(that.ofLabels);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (ofTypeLabelsSet ? 1 : 0);
        result = 31 * result + ofLabels.hashCode();
        result = 31 * result + k;
        return result;
    }
}
