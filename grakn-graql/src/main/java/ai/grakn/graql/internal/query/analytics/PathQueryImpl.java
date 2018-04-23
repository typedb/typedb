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
import ai.grakn.graql.analytics.PathQuery;

import java.util.List;
import java.util.Optional;

import static ai.grakn.util.GraqlSyntax.Compute.PATH;

class PathQueryImpl extends AbstractPathQuery<Optional<List<Concept>>, PathQuery> implements PathQuery {

    PathQueryImpl(Optional<GraknTx> tx) {
        super(tx);
    }

    @Override
    public final ComputeJob<Optional<List<Concept>>> createJob() {
        return queryRunner().run(this);
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

        if (!from().equals(that.from())) return false;
        return to().equals(that.to());
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + PATH.hashCode();
        result = 31 * result + from().hashCode();
        result = 31 * result + to().hashCode();
        return result;
    }
}
