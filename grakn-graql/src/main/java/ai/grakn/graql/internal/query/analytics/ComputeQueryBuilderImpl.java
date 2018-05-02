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
import ai.grakn.graql.analytics.ComputeQueryBuilder;
import ai.grakn.graql.analytics.CentralityQueryBuilder;
import ai.grakn.graql.analytics.ClusterQueryBuilder;
import ai.grakn.graql.analytics.ConnectedComponentQuery;
import ai.grakn.graql.analytics.CorenessQuery;
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.analytics.KCoreQuery;
import ai.grakn.graql.analytics.MaxQuery;
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.analytics.MedianQuery;
import ai.grakn.graql.analytics.MinQuery;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.analytics.StdQuery;
import ai.grakn.graql.analytics.SumQuery;
import ai.grakn.graql.internal.query.NewComputeQueryImpl;
import ai.grakn.util.GraqlSyntax;

import java.util.Map;
import java.util.Optional;

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
        return new NewComputeQueryImpl(tx, GraqlSyntax.Compute.COUNT);
    }

    @Override
    public NewComputeQuery min() {
        return new NewComputeQueryImpl(tx, GraqlSyntax.Compute.MIN);
    }

    @Override
    public NewComputeQuery max() {
        return new NewComputeQueryImpl(tx, GraqlSyntax.Compute.MAX);
    }

    @Override
    public NewComputeQuery sum() {
        return new NewComputeQueryImpl(tx, GraqlSyntax.Compute.SUM);
    }

    @Override
    public NewComputeQuery mean() {
        return new NewComputeQueryImpl(tx, GraqlSyntax.Compute.MEAN);
    }

    @Override
    public NewComputeQuery std() {
        return new NewComputeQueryImpl(tx, GraqlSyntax.Compute.STD);
    }

    @Override
    public NewComputeQuery median() {
        return new NewComputeQueryImpl(tx, GraqlSyntax.Compute.MEDIAN);
    }

    @Override
    public NewComputeQuery path() {
        return new NewComputeQueryImpl(tx, GraqlSyntax.Compute.PATH);
    }

    @Override
    public ConnectedComponentQuery<Map<String, Long>> connectedComponent() {
        return new ConnectedComponentQueryImpl<>(tx);
    }

    @Override
    public KCoreQuery kCore() {
        return new KCoreQueryImpl(tx);
    }

    @Override
    public CorenessQuery coreness() {
        return new CorenessQueryImpl(tx);
    }

    @Override
    public DegreeQuery degree() {
        return new DegreeQueryImpl(tx);
    }

    @Override
    public CentralityQueryBuilder centrality() {
        return new CentralityQueryBuilderImpl(tx);
    }

    @Override
    public ClusterQueryBuilder cluster() {
        return new ClusterQueryBuilderImpl(tx);
    }
}
