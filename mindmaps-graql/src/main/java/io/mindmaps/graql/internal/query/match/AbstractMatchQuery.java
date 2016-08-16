/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package io.mindmaps.graql.internal.query.match;

import io.mindmaps.MindmapsTransaction;
import io.mindmaps.graql.Aggregate;
import io.mindmaps.graql.AggregateQuery;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.admin.MatchQueryAdmin;
import io.mindmaps.graql.internal.query.aggregate.AggregateQueryImpl;

public abstract class AbstractMatchQuery<T> implements MatchQueryAdmin<T> {

    @Override
    public MatchQuery<T> withTransaction(MindmapsTransaction transaction) {
        return new MatchQueryTransaction<>(transaction, admin());
    }

    @Override
    public MatchQuery<T> limit(long limit) {
        return new MatchQueryLimit<>(admin(), limit);
    }

    @Override
    public MatchQuery<T> offset(long offset) {
        return new MatchQueryOffset<>(admin(), offset);
    }

    @Override
    public MatchQuery<T> distinct() {
        return new MatchQueryDistinct<>(admin());
    }

    @Override
    public final <S> AggregateQuery<S> aggregate(Aggregate<? super T, S> aggregate) {
        return new AggregateQueryImpl<>(admin(), aggregate);
    }
}
