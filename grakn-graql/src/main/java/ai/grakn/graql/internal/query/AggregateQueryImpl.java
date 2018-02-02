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

package ai.grakn.graql.internal.query;

import ai.grakn.GraknTx;
import ai.grakn.graql.AggregateQuery;
import com.google.auto.value.AutoValue;

import java.util.Optional;

/**
 * Implementation of AggregateQuery
 * @param <T> the type of the aggregate result
 */
@AutoValue
abstract class AggregateQueryImpl<T> extends AbstractExecutableQuery<T> implements AggregateQuery<T> {

    @Override
    public AggregateQuery<T> withTx(GraknTx tx) {
        return Queries.aggregate(match().withTx(tx).admin(), aggregate());
    }

    @Override
    public T execute() {
        return aggregate().apply(match().stream());
    }

    @Override
    public boolean isReadOnly() {
        //TODO An aggregate query may modify the graph if using a user-defined aggregate method. See TP # 13731.
        return true;
    }

    @Override
    public final Optional<GraknTx> tx() {
        return match().admin().tx();
    }

    @Override
    public String toString() {
        return match().toString() + " aggregate " + aggregate().toString() + ";";
    }
}
