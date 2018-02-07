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
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.analytics.CorenessQuery;
import ai.grakn.graql.internal.analytics.CorenessVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeDistributionMapReduce;
import ai.grakn.graql.internal.analytics.NoResultException;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

class CorenessQueryImpl extends AbstractCentralityQuery<CorenessQuery> implements CorenessQuery {

    private long k = 2L;

    CorenessQueryImpl(Optional<GraknTx> tx) {
        super(tx);
    }

    @Override
    protected final Map<Long, Set<String>> innerExecute(GraknTx tx, GraknComputer computer) {
        if (k < 2L) throw GraqlQueryException.kValueSmallerThanTwo();

        Set<Label> ofLabels;

        // Check if ofType is valid before returning emptyMap
        if (targetLabels().isEmpty()) {
            ofLabels = subLabels(tx);
        } else {
            ofLabels = targetLabels().stream()
                    .flatMap(typeLabel -> {
                        Type type = tx.getSchemaConcept(typeLabel);
                        if (type == null) throw GraqlQueryException.labelNotFound(typeLabel);
                        if (type.isRelationshipType()) throw GraqlQueryException.kCoreOnRelationshipType(typeLabel);
                        return type.subs();
                    })
                    .map(SchemaConcept::getLabel)
                    .collect(Collectors.toSet());
        }

        Set<Label> subLabels = Sets.union(subLabels(tx), ofLabels);

        if (!selectedTypesHaveInstance(tx)) {
            return Collections.emptyMap();
        }

        ComputerResult result;
        Set<LabelId> subLabelIds = convertLabelsToIds(tx, subLabels);
        Set<LabelId> ofLabelIds = convertLabelsToIds(tx, ofLabels);

        try {
            result = computer.compute(
                    new CorenessVertexProgram(k),
                    new DegreeDistributionMapReduce(ofLabelIds, CorenessVertexProgram.CORENESS),
                    subLabelIds);
        } catch (NoResultException e) {
            return Collections.emptyMap();
        }

        return result.memory().get(DegreeDistributionMapReduce.class.getName());
    }

    @Override
    public CorenessQuery minK(long k) {
        this.k = k;
        return this;
    }

    @Override
    public final long minK() {
        return k;
    }

    @Override
    CentralityMeasure getMethod() {
        return CentralityMeasure.K_CORE;
    }

    @Override
    String graqlString() {
        return super.graqlString() + " where k = " + k + ";";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        CorenessQueryImpl that = (CorenessQueryImpl) o;

        return k == that.k;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Long.hashCode(k);
        return result;
    }
}
