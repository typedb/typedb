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
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.GraqlConverter;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MatchAdmin;

import java.util.stream.Stream;

/**
 * Implementation of AggregateQuery
 * @param <T> the type of the aggregate result
 */
class AggregateQueryImpl<T> implements AggregateQuery<T> {

    private final MatchAdmin match;
    private final Aggregate<? super Answer, T> aggregate;

    AggregateQueryImpl(MatchAdmin match, Aggregate<? super Answer, T> aggregate) {
        this.match = match;
        this.aggregate = aggregate;
    }

    @Override
    public AggregateQuery<T> withTx(GraknTx tx) {
        return new AggregateQueryImpl<>(match.withTx(tx).admin(), aggregate);
    }

    @Override
    public T execute() {
        return aggregate.apply(match.stream());
    }

    @Override
    public <S> Stream<S> results(GraqlConverter<?, S> converter) {
        return Stream.of(converter.convert(execute()));
    }

    @Override
    public boolean isReadOnly() {
        //TODO An aggregate query may modify the graph if using a user-defined aggregate method. See TP # 13731.
        return true;
    }

    @Override
    public String toString() {
        return match.toString() + " aggregate " + aggregate.toString() + ";";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AggregateQueryImpl<?> that = (AggregateQueryImpl<?>) o;

        if (!match.equals(that.match)) return false;
        return aggregate.equals(that.aggregate);
    }

    @Override
    public int hashCode() {
        int result = match.hashCode();
        result = 31 * result + aggregate.hashCode();
        return result;
    }
}
