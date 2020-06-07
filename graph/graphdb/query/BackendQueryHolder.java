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

import com.google.common.base.Preconditions;
import grakn.core.graph.graphdb.query.profile.ProfileObservable;
import grakn.core.graph.graphdb.query.profile.QueryProfiler;

/**
 * Holds a BackendQuery and captures additional information that pertains to its execution and to be used by a
 * QueryExecutor:
 * <ul>
 *     <li>Whether the query is fitted, i.e., whether all results returned from executing the backend query are part
 *     of the result set or must be filtered in memory.</li>
 *     <li>Whether the query results will already be sorted in the user defined sort order or whether extra sorting is
 *     required.</li>
 *     <li>Additional execution info required by the query executor. This would be compiled by the query optimizer
 *     and is passed through verbatim. Can be null.</li>
 * </ul>
 *
 */
public class BackendQueryHolder<E extends BackendQuery<E>> implements ProfileObservable {

    private final E backendQuery;
    private final boolean isFitted;
    private final boolean isSorted;
    private final Object executionInfo;
    private QueryProfiler profiler = QueryProfiler.NO_OP;

    public BackendQueryHolder(E backendQuery, boolean fitted, boolean sorted, Object executionInfo) {
        Preconditions.checkArgument(backendQuery!=null);
        this.backendQuery = backendQuery;
        isFitted = fitted;
        isSorted = sorted;
        this.executionInfo = executionInfo;
    }

    public BackendQueryHolder(E backendQuery, boolean fitted, boolean sorted) {
        this(backendQuery, fitted, sorted, null);
    }

    public Object getExecutionInfo() {
        return executionInfo;
    }

    public boolean isFitted() {
        return isFitted;
    }

    public boolean isSorted() {
        return isSorted;
    }

    public E getBackendQuery() {
        return backendQuery;
    }

    public QueryProfiler getProfiler() {
        return profiler;
    }

    @Override
    public void observeWith(QueryProfiler parentProfiler) {
        Preconditions.checkArgument(parentProfiler!=null);
        this.profiler = parentProfiler.addNested(QueryProfiler.OR_QUERY);
        profiler.setAnnotation(QueryProfiler.FITTED_ANNOTATION,isFitted);
        profiler.setAnnotation(QueryProfiler.ORDERED_ANNOTATION,isSorted);
        profiler.setAnnotation(QueryProfiler.QUERY_ANNOTATION,backendQuery);
        if (backendQuery instanceof ProfileObservable) ((ProfileObservable)backendQuery).observeWith(profiler);
    }
}
