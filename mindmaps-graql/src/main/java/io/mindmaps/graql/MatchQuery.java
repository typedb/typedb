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
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.internal.query.match.*;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

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
     * @return admin instance for inspecting and manipulating this query
     */
    Admin<T> admin();

    /**
     * Admin class for inspecting and manipulating a MatchQuery
     */
    interface Admin<T> extends MatchQuery<T> {

        @Override
        default Admin<T> admin() {
            return this;
        }

        /**
         * Execute the query using the given transaction.
         * @param transaction the transaction to use to execute the query
         * @param order how to order the resulting stream
         * @return a stream of results
         */
        Stream<T> stream(Optional<MindmapsTransaction> transaction, Optional<MatchOrder> order);

        /**
         * @param transaction the transaction to use to get types from the graph
         * @return all concept types referred to explicitly in the query
         */
        Set<Type> getTypes(MindmapsTransaction transaction);

        /**
         * @return all concept types referred to explicitly in the query
         */
        Set<Type> getTypes();

        /**
         * @return the pattern to match in the graph
         */
        Pattern.Conjunction<Pattern.Admin> getPattern();

        /**
         * @return the transaction the query operates on, if one was provided
         */
        Optional<MindmapsTransaction> getTransaction();
    }
}
