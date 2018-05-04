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

import ai.grakn.GraknTx;
import ai.grakn.graql.NewComputeQuery;
import ai.grakn.graql.analytics.CentralityQueryBuilder;
import ai.grakn.graql.analytics.ClusterQueryBuilder;
import ai.grakn.graql.analytics.ComputeQueryBuilder;
import ai.grakn.graql.analytics.ConnectedComponentQuery;
import ai.grakn.graql.analytics.CorenessQuery;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.analytics.KCoreQuery;
import ai.grakn.graql.internal.query.NewComputeQueryImpl;

import java.util.Map;
import java.util.Optional;

import static ai.grakn.util.GraqlSyntax.Compute.Method;

/**
 * This class implements ComputeQueryBuilder.
 *
 * @author Jason Liu
 */

public class ComputeQueryBuilderImpl implements ComputeQueryBuilder {

    private Optional<GraknTx> tx;

    public ComputeQueryBuilderImpl(Optional<GraknTx> tx) {
        this.tx = tx;
    }

    @Override
    public ComputeQueryBuilder withTx(GraknTx tx) {
        this.tx = Optional.of(tx);
        return this;
    }

    @Override
    public NewComputeQuery count() {
        return new NewComputeQueryImpl(tx, Method.COUNT);
    }

    @Override
    public NewComputeQuery min() {
        return new NewComputeQueryImpl(tx, Method.MIN);
    }

    @Override
    public NewComputeQuery max() {
        return new NewComputeQueryImpl(tx, Method.MAX);
    }

    @Override
    public NewComputeQuery sum() {
        return new NewComputeQueryImpl(tx, Method.SUM);
    }

    @Override
    public NewComputeQuery mean() {
        return new NewComputeQueryImpl(tx, Method.MEAN);
    }

    @Override
    public NewComputeQuery std() {
        return new NewComputeQueryImpl(tx, Method.STD);
    }

    @Override
    public NewComputeQuery median() {
        return new NewComputeQueryImpl(tx, Method.MEDIAN);
    }

    @Override
    public NewComputeQuery path() {
        return new NewComputeQueryImpl(tx, Method.PATH);
    }

    @Override
    public NewComputeQuery centrality() {
        return new NewComputeQueryImpl(tx, Method.CENTRALITY);
    }

    @Override
    public ClusterQueryBuilder cluster() {
        return new ClusterQueryBuilderImpl(tx);
    }
}
