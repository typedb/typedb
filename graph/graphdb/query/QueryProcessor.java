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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.core.QueryException;
import grakn.core.graph.graphdb.query.profile.QueryProfiler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Executes a given ElementQuery against a provided QueryExecutor to produce the result set of elements.
 * <p>
 * The QueryProcessor creates a number of stacked iterators. <br>
 * At the highest level, the OuterIterator ensures that the correct (up to the given limit) number of elements is returned. It also provides the implementation of remove()
 * by calling the element's remove() method. <br>
 * The OuterIterator wraps the "unfolded" iterator which is a combination of the individual result set iterators of the sub-queries of the given query (see ElementQuery#getSubQuery(int).
 * The unfolded iterator combines this iterators by checking whether 1) the result sets need additional filtering (if so, a filter iterator is wrapped around it) and 2) whether
 * the final result set needs to be sorted and in what order. If the result set needs to be sorted and the individual sub-query result sets aren't, then a PreSortingIterator is wrapped around
 * the iterator which effectively iterates the result set out, sorts it and then returns an iterator (i.e. much more expensive than exploiting existing sort orders).<br>
 * In this way, the individual sub-result sets are prepared and then merged together the MergeSortIterator (which conserves sort order if present).
 * The semantics of the queries is OR, meaning the result sets are combined.
 * However, when ElementQuery#hasDuplicateResults() is true (which assumes that the result set is sorted) then the merge sort iterator
 * filters out immediate duplicates.
 */
public class QueryProcessor<Q extends ElementQuery<R, B>, R extends JanusGraphElement, B extends BackendQuery<B>> implements Iterable<R> {

    private static final int MAX_SORT_ITERATION = 1000000;


    private final Q query;
    private final QueryExecutor<Q, R, B> executor;

    public QueryProcessor(Q query, QueryExecutor<Q, R, B> executor) {
        Preconditions.checkNotNull(query);
        Preconditions.checkNotNull(executor);
        this.query = query;
        this.executor = executor;
    }

    @Override
    public Iterator<R> iterator() {
        if (query.isEmpty()) {
            return Collections.emptyIterator();
        }

        return new ResultSetIterator(getUnfoldedIterator(), (query.hasLimit()) ? query.getLimit() : Query.NO_LIMIT);
    }

    private Iterator<R> getUnfoldedIterator() {
        Iterator<R> iterator = null;
        boolean hasDeletions = executor.hasDeletions(query);
        Iterator<R> newElements = executor.getNew(query);
        if (query.isSorted()) {
            for (int i = query.numSubQueries() - 1; i >= 0; i--) {
                BackendQueryHolder<B> subquery = query.getSubQuery(i);
                Iterator<R> subqueryIterator = getFilterIterator((subquery.isSorted())
                                ? new LimitAdjustingIterator(subquery)
                                : new PreSortingIterator(subquery),
                        hasDeletions,
                        !subquery.isFitted());

                iterator = (iterator == null)
                        ? subqueryIterator
                        : new ResultMergeSortIterator<>(subqueryIterator, iterator, query.getSortOrder(), query.hasDuplicateResults());
            }

            Preconditions.checkArgument(iterator != null);

            if (newElements.hasNext()) {
                List<R> allNew = Lists.newArrayList(newElements);
                allNew.sort(query.getSortOrder());
                iterator = new ResultMergeSortIterator<>(allNew.iterator(), iterator, query.getSortOrder(), query.hasDuplicateResults());
            }
        } else {
            Set<R> allNew;
            if (newElements.hasNext()) {
                allNew = Sets.newHashSet(newElements);
            } else {
                allNew = ImmutableSet.of();
            }

            List<Iterator<R>> iterators = new ArrayList<>(query.numSubQueries());
            for (int i = 0; i < query.numSubQueries(); i++) {
                BackendQueryHolder<B> subquery = query.getSubQuery(i);
                Iterator<R> subIterator = new LimitAdjustingIterator(subquery);
                subIterator = getFilterIterator(subIterator, hasDeletions, !subquery.isFitted());
                if (!allNew.isEmpty()) {
                    subIterator = Iterators.filter(subIterator, r -> !allNew.contains(r));
                }
                iterators.add(subIterator);
            }
            if (iterators.size() > 1) {
                iterator = Iterators.concat(iterators.iterator());
                if (query.hasDuplicateResults()) { //Cache results and filter out duplicates
                    Set<R> seenResults = new HashSet<>();
                    iterator = Iterators.filter(iterator, r -> {
                        if (seenResults.contains(r)) return false;
                        else {
                            seenResults.add(r);
                            return true;
                        }
                    });
                }
            } else iterator = iterators.get(0);

            if (!allNew.isEmpty()) iterator = Iterators.concat(allNew.iterator(), iterator);
        }
        return iterator;
    }

    private Iterator<R> getFilterIterator(Iterator<R> iterator, boolean filterDeletions, boolean filterMatches) {
        if (filterDeletions || filterMatches) {
            return Iterators.filter(iterator, r -> (!filterDeletions || !executor.isDeleted(query, r)) && (!filterMatches || query.matches(r)));
        } else {
            return iterator;
        }
    }

    private final class PreSortingIterator implements Iterator<R> {

        private final Iterator<R> iterator;

        private PreSortingIterator(BackendQueryHolder<B> backendQueryHolder) {
            List<R> all = Lists.newArrayList(executor.execute(query,
                    backendQueryHolder.getBackendQuery().updateLimit(MAX_SORT_ITERATION),
                    backendQueryHolder.getExecutionInfo(), backendQueryHolder.getProfiler()));
            if (all.size() >= MAX_SORT_ITERATION) {
                throw new QueryException("Could not execute query since pre-sorting requires fetching more than " + MAX_SORT_ITERATION + " elements. Consider rewriting the query to exploit sort orders");
            }
            all.sort(query.getSortOrder());
            iterator = all.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public R next() {
            return iterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

     /*
     TODO: Make the returned iterator smarter about limits: If less than LIMIT elements are returned,
     it checks if the underlying iterators have been exhausted. If not, then it doubles the limit, discards the first count
     elements and returns the remaining ones. Tricky bit: how to keep track of which iterators have been exhausted?
     */


    private final class LimitAdjustingIterator extends grakn.core.graph.graphdb.query.LimitAdjustingIterator<R> {

        private B backendQuery;
        private final QueryProfiler profiler;
        private final Object executionInfo;

        private LimitAdjustingIterator(BackendQueryHolder<B> backendQueryHolder) {
            super(Integer.MAX_VALUE - 1, backendQueryHolder.getBackendQuery().getLimit());
            this.backendQuery = backendQueryHolder.getBackendQuery();
            this.executionInfo = backendQueryHolder.getExecutionInfo();
            this.profiler = backendQueryHolder.getProfiler();
        }

        @Override
        public Iterator<R> getNewIterator(int newLimit) {
            if (!backendQuery.hasLimit() || newLimit > backendQuery.getLimit()) {
                backendQuery = backendQuery.updateLimit(newLimit);
            }
            return executor.execute(query, backendQuery, executionInfo, profiler);
        }

    }

}
