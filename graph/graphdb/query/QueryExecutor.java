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
import grakn.core.graph.graphdb.query.profile.QueryProfiler;

import java.util.Iterator;

/**
 * Executes a given query and its subqueries against an underlying data store and transaction.
 */
public interface QueryExecutor<Q extends ElementQuery, R extends JanusGraphElement, B extends BackendQuery> {

    /**
     * Returns all newly created elements in a transactional context that match the given query.
     */
    Iterator<R> getNew(Q query);

    /**
     * Whether the transactional context contains any deletions that could potentially affect the result set of the given query.
     * This is used to determine whether results need to be checked for deletion with #isDeleted(ElementQuery, JanusGraphElement).
     */
    boolean hasDeletions(Q query);

    /**
     * Whether the given result entry has been deleted in the transactional context and should hence be removed from the result set.
     */
    boolean isDeleted(Q query, R result);

    /**
     * Executes the given sub-query against a data store and returns an iterator over the results. These results are not yet adjusted
     * to any modification made in the transactional context which are done by the QueryProcessor using the other methods
     */
    Iterator<R> execute(Q query, B subquery, Object executionInfo, QueryProfiler profiler);

}
