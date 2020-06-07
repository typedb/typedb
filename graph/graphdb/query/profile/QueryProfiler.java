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

package grakn.core.graph.graphdb.query.profile;

import grakn.core.graph.graphdb.query.Query;
import grakn.core.graph.graphdb.query.graph.JointIndexQuery.Subquery;

import java.util.Collection;
import java.util.function.Function;


public interface QueryProfiler {
    String CONDITION_ANNOTATION = "condition";
    String ORDERS_ANNOTATION = "orders";
    String LIMIT_ANNOTATION = "limit";

    String MULTIQUERY_ANNOTATION = "multi";
    String MULTIPREFETCH_ANNOTATION = "multiPreFetch";
    String NUMVERTICES_ANNOTATION = "vertices";
    String PARTITIONED_VERTEX_ANNOTATION = "partitioned";

    String FITTED_ANNOTATION = "isFitted";
    String ORDERED_ANNOTATION = "isOrdered";
    String QUERY_ANNOTATION = "query";
    String FULLSCAN_ANNOTATION = "fullscan";
    String INDEX_ANNOTATION = "index";

    String OR_QUERY = "OR-query";
    String AND_QUERY = "AND-query";
    String OPTIMIZATION = "optimization";

    QueryProfiler NO_OP = new QueryProfiler() {
        @Override
        public QueryProfiler addNested(String groupName) {
            return this;
        }

        @Override
        public QueryProfiler setAnnotation(String key, Object value) {
            return this;
        }

        @Override
        public void startTimer() {
        }

        @Override
        public void stopTimer() {
        }

        @Override
        public void setResultSize(long size) {
        }
    };

    QueryProfiler addNested(String groupName);

    QueryProfiler setAnnotation(String key, Object value);

    void startTimer();

    void stopTimer();

    void setResultSize(long size);

    static <Q extends Query, R extends Collection> R profile(QueryProfiler profiler, Q query, Function<Q, R> queryExecutor) {
        return profile(profiler, query, false, queryExecutor);
    }

    static <Q extends Query, R extends Collection> R profile(String groupName, QueryProfiler profiler, Q query, Function<Q, R> queryExecutor) {
        return profile(groupName, profiler, query, false, queryExecutor);
    }

    static <Q extends Query, R extends Collection> R profile(QueryProfiler profiler, Q query, boolean multiQuery, Function<Q, R> queryExecutor) {
        return profile("backend-query", profiler, query, multiQuery, queryExecutor);
    }

    static <Q extends Query, R extends Collection> R profile(String groupName, QueryProfiler profiler, Q query, boolean multiQuery, Function<Q, R> queryExecutor) {
        QueryProfiler sub = profiler.addNested(groupName);
        sub.setAnnotation(QUERY_ANNOTATION, query);
        if (query.hasLimit()) sub.setAnnotation(LIMIT_ANNOTATION, query.getLimit());
        sub.startTimer();
        R result = queryExecutor.apply(query);
        sub.stopTimer();
        long resultSize = 0;
        if (multiQuery && profiler != QueryProfiler.NO_OP) {
            //The result set is a collection of collections, but don't do this computation if profiling is disabled
            for (Object r : result) {
                if (r instanceof Collection) resultSize += ((Collection) r).size();
                else resultSize++;
            }
        } else {
            resultSize = result.size();
        }
        sub.setResultSize(resultSize);
        return result;
    }

    static QueryProfiler startProfile(QueryProfiler profiler, Subquery query) {
        QueryProfiler sub = profiler.addNested("backend-query");
        sub.setAnnotation(QUERY_ANNOTATION, query);
        if (query.hasLimit()) sub.setAnnotation(LIMIT_ANNOTATION, query.getLimit());
        sub.startTimer();
        return sub;
    }
}
