/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graph.core;


import java.util.List;


public interface QueryDescription {

    /**
     * Returns a string representation of the entire query
     */
    @Override
    String toString();

    /**
     * Returns how many individual queries are combined into this query, meaning, how many
     * queries will be executed in one batch.
     */
    int getNoCombinedQueries();

    /**
     * Returns the number of sub-queries this query is comprised of. Each sub-query represents one OR clause, i.e.,
     * the union of each sub-query's result is the overall result.
     */
    int getNoSubQueries();

    /**
     * Returns a list of all sub-queries that comprise this query
     */
    List<? extends SubQuery> getSubQueries();

    /**
     * Represents one sub-query of this query. Each sub-query represents one OR clause.
     */
    interface SubQuery {

        /**
         * Whether this query is fitted, i.e. whether the returned results must be filtered in-memory.
         */
        boolean isFitted();

        /**
         * Whether this query respects the sort order of parent query or requires sorting in-memory.
         */
        boolean isSorted();

    }


}
