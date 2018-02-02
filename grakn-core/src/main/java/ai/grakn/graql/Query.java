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

package ai.grakn.graql;

import ai.grakn.GraknTx;

import javax.annotation.CheckReturnValue;
import java.util.stream.Stream;

/**
 * A Graql query of any kind. May read and write to the graph.
 *
 * @param <T> The result type after executing the query
 *
 * @author Felix Chapman
 */
public interface Query<T> {

    /**
     * @param tx the graph to execute the query on
     * @return a new query with the graph set
     */
    @CheckReturnValue
    Query<T> withTx(GraknTx tx);

    /**
     * Execute the query against the graph (potentially writing to the graph) and return a result
     * @return the result of the query
     */
    default T execute() {
        return convert(stream());
    }

    /**
     * Execute the query and return a human-readable stream of results
     *
     * @deprecated use {@link #results(GraqlConverter)}}
     */
    @Deprecated
    @CheckReturnValue
    default Stream<String> resultsString(Printer printer) {
        return results(printer);
    }

    /**
     * Execute the query and return a converted stream of results
     */
    @CheckReturnValue
    default <S> Stream<S> results(GraqlConverter<?, S> converter) {
        return stream().map(converter::convert);
    }

    @CheckReturnValue
    // TODO: this is bad maybe
    T convert(Stream<?> results);

    @CheckReturnValue
    // TODO maybe this is bad too??
    Stream<?> stream();

    /**
     * Whether this query will modify the graph
     */
    @CheckReturnValue
    boolean isReadOnly();
}
