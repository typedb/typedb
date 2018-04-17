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
import java.util.List;
import java.util.Optional;

import static ai.grakn.graql.internal.util.StringConverter.nullableIdToString;

class PathQueryImpl extends AbstractComputeQuery<Optional<List<Concept>>, PathQuery> implements PathQuery {

    private @Nullable ConceptId sourceId = null;
    private @Nullable ConceptId destinationId = null;

    PathQueryImpl(Optional<GraknTx> tx) {
        super(tx);
    }

    @Override
    public final ComputeJob<Optional<List<Concept>>> createJob() {
        return queryRunner().run(this);
    }

    @Override
    public PathQuery from(ConceptId sourceId) {
        this.sourceId = sourceId;
        return this;
    }

    @Override
    public final ConceptId from() {
        if (sourceId == null) throw GraqlQueryException.noPathSource();
        return sourceId;
    }

    @Override
    public PathQuery to(ConceptId destinationId) {
        this.destinationId = destinationId;
        return this;
    }

    @Override
    public final ConceptId to() {
        if (destinationId == null) throw GraqlQueryException.noPathDestination();
        return destinationId;
    }

    @Override
    final String graqlString() {
        return "path from " + nullableIdToString(sourceId) + " to " + nullableIdToString(destinationId)
                + subtypeString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PathQueryImpl pathQuery = (PathQueryImpl) o;

        if (sourceId != null ? !sourceId.equals(pathQuery.sourceId) : pathQuery.sourceId != null) return false;
        return destinationId != null ? destinationId.equals(pathQuery.destinationId) : pathQuery.destinationId == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (sourceId != null ? sourceId.hashCode() : 0);
        result = 31 * result + (destinationId != null ? destinationId.hashCode() : 0);
        return result;
    }
}
