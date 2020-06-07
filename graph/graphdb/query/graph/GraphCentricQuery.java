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
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.graphdb.internal.ElementCategory;
import grakn.core.graph.graphdb.internal.OrderList;
import grakn.core.graph.graphdb.query.BackendQueryHolder;
import grakn.core.graph.graphdb.query.BaseQuery;
import grakn.core.graph.graphdb.query.ElementQuery;
import grakn.core.graph.graphdb.query.condition.Condition;
import grakn.core.graph.graphdb.query.condition.FixedCondition;
import grakn.core.graph.graphdb.query.profile.ProfileObservable;
import grakn.core.graph.graphdb.query.profile.QueryProfiler;
import org.apache.commons.collections.comparators.ComparableComparator;

import java.util.Comparator;
import java.util.Objects;

/**
 * An executable ElementQuery for JanusGraphQuery. This query contains
 * the condition, and only one sub-query JointIndexQuery.
 * It also maintains the ordering for the query result which is needed by the QueryProcessor
 * to correctly order the result.
 */
public class GraphCentricQuery extends BaseQuery implements ElementQuery<JanusGraphElement, JointIndexQuery>, ProfileObservable {

    /*
     * The condition of this query, the result set is the set of all elements in the graph for which this
     * condition evaluates to true.
     */
    private final Condition<JanusGraphElement> condition;
    /**
     * The JointIndexQuery to execute against the indexing backends and index store.
     */
    private final BackendQueryHolder<JointIndexQuery> indexQuery;
    /**
     * The result order of this query (if any)
     */
    private final OrderList orders;
    /**
     * The type of element this query is asking for: vertex, edge, or property.
     */
    private final ElementCategory resultType;

    public GraphCentricQuery(ElementCategory resultType, Condition<JanusGraphElement> condition, OrderList orders,
                             BackendQueryHolder<JointIndexQuery> indexQuery, int limit) {
        super(limit);
        Preconditions.checkNotNull(condition);
        Preconditions.checkArgument(orders != null && orders.isImmutable());
        Preconditions.checkNotNull(resultType);
        Preconditions.checkNotNull(indexQuery);
        this.condition = condition;
        this.orders = orders;
        this.resultType = resultType;
        this.indexQuery = indexQuery;
    }

    static GraphCentricQuery emptyQuery(ElementCategory resultType) {
        Condition<JanusGraphElement> cond = new FixedCondition<>(false);
        return new GraphCentricQuery(resultType, cond, OrderList.NO_ORDER,
                new BackendQueryHolder<>(new JointIndexQuery(),
                        true, false), 0);
    }

    public Condition<JanusGraphElement> getCondition() {
        return condition;
    }

    public ElementCategory getResultType() {
        return resultType;
    }

    public OrderList getOrder() {
        return orders;
    }

    @Override
    public String toString() {
        final StringBuilder b = new StringBuilder();
        b.append("[").append(condition.toString()).append("]");
        if (!orders.isEmpty()) b.append(getLimit());
        if (hasLimit()) b.append("(").append(getLimit()).append(")");
        b.append(":").append(resultType.toString());
        return b.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(condition, resultType, orders, getLimit());
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
        GraphCentricQuery oth = (GraphCentricQuery) other;
        return resultType == oth.resultType && condition.equals(oth.condition) &&
                orders.equals(oth.getOrder()) && getLimit() == oth.getLimit();
    }

    @Override
    public boolean isEmpty() {
        return getLimit() <= 0;
    }

    @Override
    public int numSubQueries() {
        return 1;
    }

    @Override
    public BackendQueryHolder<JointIndexQuery> getSubQuery(int position) {
        if (position == 0) return indexQuery;
        else throw new IndexOutOfBoundsException();
    }

    @Override
    public boolean isSorted() {
        return !orders.isEmpty();
    }

    @Override
    public Comparator<JanusGraphElement> getSortOrder() {
        if (orders.isEmpty()) return new ComparableComparator();
        else return orders;
    }

    @Override
    public boolean hasDuplicateResults() {
        return false;
    }

    @Override
    public boolean matches(JanusGraphElement element) {
        return condition.evaluate(element);
    }


    @Override
    public void observeWith(QueryProfiler profiler) {
        profiler.setAnnotation(QueryProfiler.CONDITION_ANNOTATION, condition);
        profiler.setAnnotation(QueryProfiler.ORDERS_ANNOTATION, orders);
        if (hasLimit()) profiler.setAnnotation(QueryProfiler.LIMIT_ANNOTATION, getLimit());
        indexQuery.observeWith(profiler);
    }
}
