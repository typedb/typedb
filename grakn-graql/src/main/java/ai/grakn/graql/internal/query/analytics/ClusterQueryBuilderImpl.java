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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.GraknTx;
import ai.grakn.graql.analytics.ClusterQueryBuilder;
import ai.grakn.graql.analytics.ConnectedComponentQuery;
import ai.grakn.graql.analytics.KCoreQuery;

import java.util.Map;
import java.util.Optional;

/**
 * This class implements ClusterQueryBuilder.
 *
 * @author Jason Liu
 */

public class ClusterQueryBuilderImpl implements ClusterQueryBuilder {

    private Optional<GraknTx> tx;

    ClusterQueryBuilderImpl(Optional<GraknTx> tx) {
        this.tx = tx;
    }

    @Override
    public ClusterQueryBuilder withTx(GraknTx tx) {
        this.tx = Optional.of(tx);
        return this;
    }

    @Override
    public KCoreQuery usingKCore() {
        return new KCoreQueryImpl(tx);
    }

    @Override
    public ConnectedComponentQuery<Map<String, Long>> usingConnectedComponent() {
        return new ConnectedComponentQueryImpl<>(tx);
    }
}
