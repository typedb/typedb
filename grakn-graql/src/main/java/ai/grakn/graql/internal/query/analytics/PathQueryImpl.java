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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.ComputeJob;
import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.analytics.PathQuery;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static ai.grakn.graql.internal.util.StringConverter.nullableIdToString;
import static ai.grakn.util.GraqlSyntax.COMMA_SPACE;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.FROM;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.TO;
import static ai.grakn.util.GraqlSyntax.Compute.PATH;
import static ai.grakn.util.GraqlSyntax.SPACE;
import static java.util.stream.Collectors.joining;

class PathQueryImpl extends AbstractComputeQuery<Optional<List<Concept>>, PathQuery> implements PathQuery {

    private @Nullable ConceptId from = null;
    private @Nullable ConceptId to = null;

    PathQueryImpl(Optional<GraknTx> tx) {
        super(tx);
    }

    @Override
    public final ComputeJob<Optional<List<Concept>>> createJob() {
        return queryRunner().run(this);
    }

    @Override
    public PathQuery from(ConceptId sourceId) {
        this.from = sourceId;
        return this;
    }

    @Override
    public final ConceptId from() {
        if (from == null) throw GraqlQueryException.noPathSource();
        return from;
    }

    @Override
    public PathQuery to(ConceptId destinationId) {
        this.to = destinationId;
        return this;
    }

    @Override
    public final ConceptId to() {
        if (to == null) throw GraqlQueryException.noPathDestination();
        return to;
    }

    @Override
    final String methodString() {
        return PATH;
    }

    @Override
    final String conditionsString() {
        List<String> conditionsList = new ArrayList<>();

        conditionsList.add(FROM + SPACE + nullableIdToString(from));
        conditionsList.add(TO + SPACE + nullableIdToString(to));
        if (!inTypesString().isEmpty()) conditionsList.add(inTypesString());

        return conditionsList.stream().collect(joining(COMMA_SPACE));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PathQueryImpl that = (PathQueryImpl) o;

        if (from != null ? !from.equals(that.from) : that.from != null) return false;
        return to != null ? to.equals(that.to) : that.to == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + PATH.hashCode();
        result = 31 * result + from.hashCode();
        result = 31 * result + to.hashCode();
        return result;
    }
}
