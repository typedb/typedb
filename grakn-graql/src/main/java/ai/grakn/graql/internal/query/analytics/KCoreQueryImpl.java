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
import ai.grakn.graql.analytics.KCoreQuery;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.K_CORE;
import static ai.grakn.util.GraqlSyntax.Compute.Arg.K;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.USING;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.WHERE;
import static ai.grakn.util.GraqlSyntax.EQUAL;
import static ai.grakn.util.GraqlSyntax.SPACE;

class KCoreQueryImpl extends AbstractClusterQuery<Map<String, Set<String>>, KCoreQuery> implements KCoreQuery {

    private static long DEFAULT_K = 2L;
    private Optional<Long> k = Optional.empty();

    KCoreQueryImpl(Optional<GraknTx> tx) {
        super(tx);
    }

    @Override
    public final ComputeJob<Map<String, Set<String>>> executeComputer() {
        return queryComputer().run(this);
    }

    @Override
    public final KCoreQuery k(long kValue) {
        k = Optional.of(kValue);
        return this;
    }

    @Override
    public final long k() {
        return k.orElse(DEFAULT_K);
    }

    @Override
    String algorithmString() {
        return USING + SPACE + K_CORE;
    }

    @Override
    String argumentsString(){
        if(k.isPresent()) return WHERE + SPACE + K + EQUAL + k.get();

        return "";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        KCoreQueryImpl that = (KCoreQueryImpl) o;

        return k() == that.k();
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + K_CORE.hashCode();
        result = 31 * result + Long.hashCode(k());
        return result;
    }
}
