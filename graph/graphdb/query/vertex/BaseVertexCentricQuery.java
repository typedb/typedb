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

package grakn.core.graph.graphdb.query.vertex;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.graphdb.internal.OrderList;
import grakn.core.graph.graphdb.query.BackendQueryHolder;
import grakn.core.graph.graphdb.query.BaseQuery;
import grakn.core.graph.graphdb.query.QueryUtil;
import grakn.core.graph.graphdb.query.condition.Condition;
import grakn.core.graph.graphdb.query.condition.FixedCondition;
import grakn.core.graph.graphdb.query.profile.ProfileObservable;
import grakn.core.graph.graphdb.query.profile.QueryProfiler;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.ArrayList;
import java.util.List;

/**
 * The base implementation for VertexCentricQuery which does not yet contain a reference to the
 * base vertex of the query. This query is constructed by BasicVertexCentricQueryBuilder#constructQuery(RelationCategory)
 * and then later extended by single or multi-vertex query which add the vertex to the query.
 * <p>
 * This class override many methods in ElementQuery - check there
 * for a description.
 */
public class BaseVertexCentricQuery extends BaseQuery implements ProfileObservable {

    /**
     * The condition of this query in QNF
     */
    protected final Condition<JanusGraphRelation> condition;
    /**
     * The individual component SliceQuery of this query. This query is considered an OR
     * of the individual components (possibly filtered by the condition if not fitted).
     */
    protected final List<BackendQueryHolder<SliceQuery>> queries;
    /**
     * The result order of this query (if any)
     */
    private final OrderList orders;
    /**
     * The direction condition of this query. This is duplicated from the condition for efficiency reasons.
     */
    protected final Direction direction;

    public BaseVertexCentricQuery(Condition<JanusGraphRelation> condition, Direction direction,
                                  List<BackendQueryHolder<SliceQuery>> queries, OrderList orders,
                                  int limit) {
        super(limit);
        Preconditions.checkArgument(condition != null && queries != null && direction != null);
        Preconditions.checkArgument(QueryUtil.isQueryNormalForm(condition) && limit >= 0);
        this.condition = condition;
        this.queries = queries;
        this.orders = orders;
        this.direction = direction;
    }

    protected BaseVertexCentricQuery(BaseVertexCentricQuery query) {
        this(query.getCondition(), query.getDirection(), query.getQueries(), query.getOrders(), query.getLimit());
    }

    /**
     * Construct an empty query
     */
    protected BaseVertexCentricQuery() {
        this(new FixedCondition<>(false), Direction.BOTH, new ArrayList<>(0), OrderList.NO_ORDER, 0);
    }

    public static BaseVertexCentricQuery emptyQuery() {
        return new BaseVertexCentricQuery();
    }

    public Condition<JanusGraphRelation> getCondition() {
        return condition;
    }

    public OrderList getOrders() {
        return orders;
    }

    public Direction getDirection() {
        return direction;
    }

    protected List<BackendQueryHolder<SliceQuery>> getQueries() {
        return queries;
    }

    public boolean isEmpty() {
        return getLimit() <= 0;
    }

    public int numSubQueries() {
        return queries.size();
    }

    /**
     * A query is considered 'simple' if it is comprised of just one sub-query and that query
     * is fitted (i.e. does not require an in-memory filtering).
     */
    public boolean isSimple() {
        return queries.size() == 1 && queries.get(0).isFitted() && queries.get(0).isSorted();
    }

    public BackendQueryHolder<SliceQuery> getSubQuery(int position) {
        return queries.get(position);
    }

    public boolean matches(JanusGraphRelation relation) {
        return condition.evaluate(relation);
    }

    @Override
    public String toString() {
        String s = "[" + condition.toString() + "]";
        if (hasLimit()) s += ":" + getLimit();
        return s;
    }

    @Override
    public void observeWith(QueryProfiler profiler) {
        profiler.setAnnotation(QueryProfiler.CONDITION_ANNOTATION, condition);
        profiler.setAnnotation(QueryProfiler.ORDERS_ANNOTATION, orders);
        if (hasLimit()) profiler.setAnnotation(QueryProfiler.LIMIT_ANNOTATION, getLimit());
        queries.forEach(bqh -> bqh.observeWith(profiler));
    }
}
