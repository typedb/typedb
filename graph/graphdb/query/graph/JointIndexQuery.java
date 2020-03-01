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

package grakn.core.graph.graphdb.query.graph;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import grakn.core.graph.diskstorage.indexing.IndexQuery;
import grakn.core.graph.graphdb.query.BackendQuery;
import grakn.core.graph.graphdb.query.BaseQuery;
import grakn.core.graph.graphdb.query.profile.ProfileObservable;
import grakn.core.graph.graphdb.query.profile.QueryProfiler;
import grakn.core.graph.graphdb.types.CompositeIndexType;
import grakn.core.graph.graphdb.types.IndexType;
import grakn.core.graph.graphdb.types.MixedIndexType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A component/sub-query of a GraphCentricQuery that gets executed against an indexing backend or the index store
 * by the query processor of the enclosing transaction.
 * <p>
 * This query itself can contain multiple sub-queries which are individually executed by the IndexSerializer
 * and the result sets merged.
 * <p>
 * Those sub-queries are either targeting an external indexing backend or the internal index store which is a distinction this
 * query keeps track of through the sub-class Subquery, since their definition and execution differs starkly.
 */
public class JointIndexQuery extends BaseQuery implements BackendQuery<JointIndexQuery>, ProfileObservable {

    private final List<Subquery> queries;

    private JointIndexQuery(List<Subquery> queries) {
        this.queries = Preconditions.checkNotNull(queries);
    }

    public JointIndexQuery() {
        this.queries = new ArrayList<>(4);
    }

    public void add(MixedIndexType index, IndexQuery query) {
        queries.add(new Subquery(index, query));
    }

    public void add(CompositeIndexType index, MultiKeySliceQuery query) {
        queries.add(new Subquery(index, query));
    }

    public int size() {
        return queries.size();
    }

    public Subquery getQuery(int pos) {
        return queries.get(pos);
    }

    public boolean isEmpty() {
        return queries.isEmpty();
    }

    @Override
    public void observeWith(QueryProfiler profiler) {
        queries.forEach(q -> q.observeWith(profiler));
    }

    @Override
    public int hashCode() {
        return Objects.hash(queries);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other == null) {
            return false;
        } else if (!getClass().isInstance(other)) {
            return false;
        }
        JointIndexQuery oth = (JointIndexQuery) other;
        return oth.queries.equals(queries);
    }

    @Override
    public String toString() {
        return queries.toString();
    }

    @Override
    public JointIndexQuery updateLimit(int newLimit) {
        JointIndexQuery ji = new JointIndexQuery(Lists.newArrayList(queries));
        ji.setLimit(newLimit);
        return ji;
    }

    public static class Subquery implements BackendQuery<Subquery>, ProfileObservable {

        private final IndexType index;
        private final BackendQuery query;
        private QueryProfiler profiler = QueryProfiler.NO_OP;

        private Subquery(IndexType index, BackendQuery query) {
            this.index = index;
            this.query = query;
        }

        public void observeWith(QueryProfiler prof) {
            this.profiler = prof.addNested(QueryProfiler.AND_QUERY);
            profiler.setAnnotation(QueryProfiler.QUERY_ANNOTATION, query);
            profiler.setAnnotation(QueryProfiler.INDEX_ANNOTATION, index.getName());
            if (index.isMixedIndex()) {
                profiler.setAnnotation(QueryProfiler.INDEX_ANNOTATION + "_impl", index.getBackingIndexName());
            }
        }

        public QueryProfiler getProfiler() {
            return profiler;
        }

        public IndexType getIndex() {
            return index;
        }

        public IndexQuery getMixedQuery() {
            Preconditions.checkArgument(index.isMixedIndex() && query instanceof IndexQuery);
            return (IndexQuery) query;
        }

        public MultiKeySliceQuery getCompositeQuery() {
            Preconditions.checkArgument(index.isCompositeIndex() && query instanceof MultiKeySliceQuery);
            return (MultiKeySliceQuery) query;
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, query);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            } else if (other == null) {
                return false;
            } else if (!getClass().isInstance(other)) {
                return false;
            }
            Subquery oth = (Subquery) other;
            return index.equals(oth.index) && query.equals(oth.query);
        }

        @Override
        public String toString() {
            return index.toString() + ":" + query.toString();
        }

        @Override
        public Subquery updateLimit(int newLimit) {
            return new Subquery(index, query.updateLimit(newLimit));
        }

        @Override
        public boolean hasLimit() {
            return query.hasLimit();
        }

        @Override
        public int getLimit() {
            return query.getLimit();
        }
    }

}
