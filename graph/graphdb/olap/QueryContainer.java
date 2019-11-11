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

package grakn.core.graph.graphdb.olap;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.graph.core.RelationType;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.graphdb.internal.RelationCategory;
import grakn.core.graph.graphdb.query.BackendQueryHolder;
import grakn.core.graph.graphdb.query.JanusGraphPredicate;
import grakn.core.graph.graphdb.query.vertex.BaseVertexCentricQuery;
import grakn.core.graph.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class QueryContainer {

    public static final int DEFAULT_HARD_QUERY_LIMIT = 100000;
    public static final String QUERY_NAME_PREFIX = "query$";

    private final StandardJanusGraphTx tx;
    private final int hardQueryLimit;

    private final Set<Query> queries;
    private final SetMultimap<SliceQuery, Query> inverseQueries;

    public QueryContainer(StandardJanusGraphTx tx) {
        this.tx = Preconditions.checkNotNull(tx);
        queries = new HashSet<>(6);
        inverseQueries = HashMultimap.create();
        hardQueryLimit = DEFAULT_HARD_QUERY_LIMIT;
    }

    public JanusGraphTransaction getTransaction() {
        return tx;
    }

    public QueryBuilder addQuery() {
        return new QueryBuilder();
    }

    Set<Query> getQueries(SliceQuery slice) {
        return inverseQueries.get(slice);
    }

    Iterable<Query> getQueries() {
        return queries;
    }

    public List<SliceQuery> getSliceQueries() {
        List<SliceQuery> slices = new ArrayList<>(queries.size() * 2);
        for (QueryContainer.Query q : getQueries()) {
            for (SliceQuery slice : q.getSlices()) {
                if (!slices.contains(slice)) slices.add(slice);
            }
        }
        return slices;
    }

    static class Query {

        private final List<SliceQuery> slices;
        private final RelationCategory returnType;

        public Query(List<SliceQuery> slices, RelationCategory returnType) {
            this.slices = slices;
            this.returnType = returnType;
        }

        public List<SliceQuery> getSlices() {
            return slices;
        }

        public RelationCategory getReturnType() {
            return returnType;
        }
    }

    public class QueryBuilder extends BasicVertexCentricQueryBuilder<QueryBuilder> {

        private QueryBuilder() {
            super(QueryContainer.this.tx);
        }

        private Query relations(RelationCategory returnType) {

            BaseVertexCentricQuery vq = super.constructQuery(returnType);
            List<SliceQuery> slices = new ArrayList<>(vq.numSubQueries());
            for (int i = 0; i < vq.numSubQueries(); i++) {
                BackendQueryHolder<SliceQuery> bq = vq.getSubQuery(i);
                SliceQuery sq = bq.getBackendQuery();
                slices.add(sq.updateLimit(bq.isFitted() ? vq.getLimit() : hardQueryLimit));
            }
            Query q = new Query(slices, returnType);
            synchronized (queries) {
                Preconditions.checkArgument(!queries.contains(q), "Query has already been added: %s", q);
                queries.add(q);
                for (SliceQuery sq : slices) {
                    inverseQueries.put(sq, q);
                }
            }
            return q;

        }

        @Override
        protected QueryBuilder getThis() {
            return this;
        }

        public void edges() {
            relations(RelationCategory.EDGE);
        }

        public void relations() {
            relations(RelationCategory.RELATION);
        }

        public void properties() {
            relations(RelationCategory.PROPERTY);
        }

        /*
        ########### SIMPLE OVERWRITES ##########
         */

        @Override
        public QueryBuilder has(String type, Object value) {
            super.has(type, value);
            return this;
        }

        @Override
        public QueryBuilder hasNot(String key, Object value) {
            super.hasNot(key, value);
            return this;
        }

        @Override
        public QueryBuilder has(String key) {
            super.has(key);
            return this;
        }

        @Override
        public QueryBuilder hasNot(String key) {
            super.hasNot(key);
            return this;
        }

        @Override
        public QueryBuilder has(String key, JanusGraphPredicate predicate, Object value) {
            super.has(key, predicate, value);
            return this;
        }

        @Override
        public <T extends Comparable<?>> QueryBuilder interval(String key, T start, T end) {
            super.interval(key, start, end);
            return this;
        }

        @Override
        public QueryBuilder types(RelationType... types) {
            super.types(types);
            return this;
        }

        @Override
        public QueryBuilder labels(String... labels) {
            super.labels(labels);
            return this;
        }

        @Override
        public QueryBuilder keys(String... keys) {
            super.keys(keys);
            return this;
        }

        public QueryBuilder type(RelationType type) {
            super.type(type);
            return this;
        }

        @Override
        public QueryBuilder direction(Direction d) {
            super.direction(d);
            return this;
        }

        @Override
        public QueryBuilder limit(int limit) {
            super.limit(limit);
            return this;
        }

        @Override
        public QueryBuilder orderBy(String key, Order order) {
            super.orderBy(key, order);
            return this;
        }


    }


}
