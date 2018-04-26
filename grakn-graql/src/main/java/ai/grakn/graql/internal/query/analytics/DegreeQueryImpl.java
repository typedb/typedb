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
import ai.grakn.graql.analytics.DegreeQuery;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.DEGREE;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.USING;
import static ai.grakn.util.GraqlSyntax.SPACE;

class DegreeQueryImpl extends AbstractCentralityQuery<DegreeQuery> implements DegreeQuery {

    DegreeQueryImpl(Optional<GraknTx> tx) {
        super(tx);
    }

    @Override
    public final ComputeJob<Map<Long, Set<String>>> run() {
        return executor().run(this);
    }

    @Override
    String algorithmString() {
        return USING + SPACE + DEGREE;
    }

    @Override
    String argumentsString() {
        return "";
    }
}
