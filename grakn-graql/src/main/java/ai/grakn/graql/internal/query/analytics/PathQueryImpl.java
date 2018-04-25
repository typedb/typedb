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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static ai.grakn.util.GraqlSyntax.COMMA_SPACE;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.FROM;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.TO;
import static ai.grakn.util.GraqlSyntax.Compute.PATH;
import static ai.grakn.util.GraqlSyntax.QUOTE;
import static ai.grakn.util.GraqlSyntax.SPACE;
import static java.util.stream.Collectors.joining;

class PathQueryImpl extends AbstractComputeQuery<List<List<Concept>>, PathQuery> implements PathQuery {

    private Optional<ConceptId> from = Optional.empty();
    private Optional<ConceptId> to = Optional.empty();

    PathQueryImpl(Optional<GraknTx> tx) {
        super(tx);
    }

    @Override
    public final ComputeJob<List<List<Concept>>> run() {
        if (!from.isPresent() || to.isPresent()) throw GraqlQueryException.invalidComputePathMissingCondition();

        return queryComputer().run(this);
    }


    public final PathQuery from(ConceptId fromID) {
        this.from = Optional.of(fromID);
        return this;
    }

    public final Optional<ConceptId> from() {
        return from;
    }

    public PathQuery to(ConceptId toID) {
        this.to = Optional.of(toID);
        return this;
    }

    public final Optional<ConceptId> to() {
        if (to == null) throw GraqlQueryException.noPathDestination();
        return to;
    }

    final String conditionsString() {
        List<String> conditionsList = new ArrayList<>();

        if (from.isPresent()) conditionsList.add(FROM + SPACE + QUOTE + from.get() + QUOTE);
        if (to.isPresent()) conditionsList.add(TO + SPACE + QUOTE + to.get() + QUOTE);
        if (!inTypesString().isEmpty()) conditionsList.add(inTypesString());

        return conditionsList.stream().collect(joining(COMMA_SPACE));
    }

    @Override
    final String methodString() {
        return PATH;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PathQueryImpl that = (PathQueryImpl) o;

        if (!this.from.equals(that.from())) return false;
        if (!this.to.equals(that.to())) return false;

        return true;
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
