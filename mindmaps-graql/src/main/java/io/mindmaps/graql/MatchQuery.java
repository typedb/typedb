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

package io.mindmaps.graql;

import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.graql.internal.admin.MatchQueryAdmin;
import io.mindmaps.graql.internal.query.aggregate.AggregateQueryImpl;
import io.mindmaps.graql.internal.query.match.*;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * a query used for finding data in a graph that matches the given patterns.
 * <p>
 * The {@code MatchQuery} is a pattern-matching query. The patterns are described in a declarative fashion, forming a
 * subgraph, then the {@code MatchQuery} will traverse the graph in an efficient fashion to find any matching subgraphs.
 *
 * @param <T> the type of the results of the query
 */
public interface MatchQuery<T> extends Streamable<T> {

    default Stream<T> stream() {
        return admin().stream(Optional.empty(), Optional.empty());
    }

    /**
     * @param transaction the transaction to execute the query on
     * @return a new MatchQuery with the transaction set
     */
    default MatchQuery<T> withTransaction(MindmapsTransaction transaction) {
        return new MatchQueryTransaction<>(transaction, admin());
    }

    /**
     * @param limit the maximum number of results the query should return
     * @return a new MatchQuery with the limit set
     */
    default MatchQuery<T> limit(long limit) {
        return new MatchQueryLimit<>(admin(), limit);
    }

    /**
     * @param offset the number of results to skip
     * @return a new MatchQuery with the offset set
     */
    default MatchQuery<T> offset(long offset) {
        return new MatchQueryOffset<>(admin(), offset);
    }

    /**
     * remove any duplicate results from the query
     * @return a new MatchQuery without duplicate results
     */
    default MatchQuery<T> distinct() {
        return new MatchQueryDistinct<>(admin());
    }

    /**
     * Aggregate results of a query.
     * @param aggregate the aggregate operation to apply
     * @param <S> the type of the aggregate result
     * @return a query that will yield the aggregate result
     */
    default <S> AggregateQuery<S> aggregate(Aggregate<? super T, S> aggregate) {
        return new AggregateQueryImpl<>(admin(), aggregate);
    }

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    MatchQueryAdmin<T> admin();

}
