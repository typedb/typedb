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
import ai.grakn.graql.analytics.CorenessQuery;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.K_CORE;
import static ai.grakn.util.GraqlSyntax.Compute.Arg.MIN_K;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.USING;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.WHERE;
import static ai.grakn.util.GraqlSyntax.EQUAL;
import static ai.grakn.util.GraqlSyntax.SPACE;

class CorenessQueryImpl extends AbstractCentralityQuery<CorenessQuery> implements CorenessQuery {

    private Optional<Long> minK = Optional.empty();
    private final static long DEFAULT_K = 2L;

    CorenessQueryImpl(Optional<GraknTx> tx) {
        super(tx);
    }

    @Override
    public final ComputeJob<Map<Long, Set<String>>> run() {
        return executor().run(this);
    }

    @Override
    public CorenessQuery minK(long k) {
        this.minK = Optional.of(k);
        return this;
    }

    @Override
    public final long minK() {
        return minK.orElse(DEFAULT_K);
    }

    @Override
    String algorithmString() {
        return USING + SPACE + K_CORE;
    }

    @Override
    String argumentsString() {
        if (minK.isPresent()) return WHERE + SPACE + MIN_K + EQUAL + minK.get();

        return "";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        CorenessQueryImpl that = (CorenessQueryImpl) o;

        return minK() == that.minK();
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Long.hashCode(minK());
        return result;
    }
}
