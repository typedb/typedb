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

package ai.grakn.graql;

import ai.grakn.GraknTx;
import ai.grakn.QueryExecutor;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.answer.Answer;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Graql query of any kind. May read and write to the graph.
 *
 * @param <T> The result type after executing the query
 *
 * @author Grakn Warriors
 */
public interface Query<T extends Answer> extends Iterable<T> {

    /**
     * @param tx the graph to execute the query on
     * @return a new query with the graph set
     */
    @CheckReturnValue
    Query<T> withTx(GraknTx tx);

    /**
     * @return a {@link Stream} of T, where T is a special type of {@link Answer}
     */
    @CheckReturnValue
    Stream<T> stream();

    /**
     * @return a {@link List} of T, where T is a special type of {@link Answer}
     */
    @CheckReturnValue
    default List<T> execute() {
        return stream().collect(Collectors.toList());
    }

    /**
     * @return an {@link Iterator} of T, where T is a special type of {@link Answer}
     */
    @Override
    @CheckReturnValue
    default Iterator<T> iterator() {
        return stream().iterator();
    }

    /**
     * @return the special type of {@link QueryExecutor}, depending on whether the query is executed on the client or
     * server side.
     */
    default QueryExecutor executor() {
        if (tx() == null) throw GraqlQueryException.noTx();
        return tx().admin().queryExecutor();
    }

    /**
     * @return boolean that indicates whether this query will modify the graph
     */
    @CheckReturnValue
    boolean isReadOnly();

    /**
     * @return the transaction {@link GraknTx} associated with this query
     */
    @Nullable
    GraknTx tx();

    /**
     * @return boolean that indicates whether this query will perform rule-based inference during execution
     */
    Boolean inferring();
}
