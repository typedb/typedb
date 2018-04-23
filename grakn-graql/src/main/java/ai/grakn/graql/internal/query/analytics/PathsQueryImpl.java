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
import ai.grakn.graql.analytics.PathsQuery;

import java.util.List;
import java.util.Optional;

import static ai.grakn.util.GraqlSyntax.Compute.PATHS;

class PathsQueryImpl extends AbstractPathQuery<List<List<Concept>>, PathsQuery> implements PathsQuery {

    PathsQueryImpl(Optional<GraknTx> tx) {
        super(tx);
    }

    @Override
    public final ComputeJob<List<List<Concept>>> createJob() {
        return queryRunner().run(this);
    }

    @Override
    final String methodString() {
        return PATHS;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PathsQueryImpl that = (PathsQueryImpl) o;

        if (from() != null ? !from().equals(that.from()) : that.from() != null) return false;
        return to() != null ? to().equals(that.to()) : that.to() == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + PATHS.hashCode();
        result = 31 * result + from().hashCode();
        result = 31 * result + to().hashCode();
        return result;
    }
}
