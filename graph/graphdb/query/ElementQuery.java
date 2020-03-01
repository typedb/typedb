/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graph.graphdb.query;

import grakn.core.graph.core.JanusGraphElement;

import java.util.Comparator;

/**
 * A query that returns JanusGraphElements. This query can consist of multiple sub-queries that together
 * form the desired result set.
 *
 */

public interface ElementQuery<R extends JanusGraphElement,B extends BackendQuery<B>> extends Query {

    /**
     * Whether the combination of the individual sub-queries can result in duplicate
     * results. Indicates to the query executor whether the results need to be de-duplicated
     *
     * @return true, if duplicate results are possible, else false
     */
    boolean hasDuplicateResults();

    /**
     * Whether the result set of this query is empty
     *
     * @return
     */
    boolean isEmpty();

    /**
     * Returns the number of sub-queries this query is comprised of.
     *
     * @return
     */
    int numSubQueries();

    /**
     * Returns the backend query at the given position that comprises this ElementQuery
     * @param position
     * @return
     */
    BackendQueryHolder<B> getSubQuery(int position);

    /**
     * Whether the given element matches the conditions of this query.
     * <p>
     * Used for result filtering if the result set returned by the query executor is not fitted.
     *
     * @param element
     * @return
     */
    boolean matches(R element);

    /**
     * Whether this query expects the results to be in a particular sort order.
     *
     * @return
     */
    boolean isSorted();

    /**
     * Returns the expected sort order of this query if any was specified. Check #isSorted() first.
     * @return
     */
    Comparator<R> getSortOrder();

}
