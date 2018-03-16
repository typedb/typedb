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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.GraknTx;
import ai.grakn.graql.ComputeQueryBuilder;
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
import ai.grakn.graql.analytics.PathsQuery;
import ai.grakn.graql.analytics.StdQuery;
import ai.grakn.graql.analytics.SumQuery;

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
    public CountQuery count() {
        return new CountQueryImpl(tx);
    }

    @Override
    public MinQuery min() {
        return new MinQueryImpl(tx);
    }

    @Override
    public MaxQuery max() {
        return new MaxQueryImpl(tx);
    }

    @Override
    public SumQuery sum() {
        return new SumQueryImpl(tx);
    }

    @Override
    public MeanQuery mean() {
        return new MeanQueryImpl(tx);
    }

    @Override
    public StdQuery std() {
        return new StdQueryImpl(tx);
    }

    @Override
    public MedianQuery median() {
        return new MedianQueryImpl(tx);
    }

    @Override
    public PathQuery path() {
        return new PathQueryImpl(tx);
    }

    @Override
    public PathsQuery paths() {
        return new PathsQueryImpl(tx);
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
