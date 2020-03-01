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

package grakn.core.graph.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.graphdb.internal.Order;
import grakn.core.graph.graphdb.query.BackendQuery;
import grakn.core.graph.graphdb.query.BaseQuery;
import grakn.core.graph.graphdb.query.Query;
import grakn.core.graph.graphdb.query.condition.Condition;

import java.util.List;
import java.util.Objects;

/**
 * An external index query executed on an IndexProvider.
 * <p>
 * A query is comprised of the store identifier against which the query ought to be executed and a query condition
 * which defines which entries match the query.
 */

public class IndexQuery extends BaseQuery implements BackendQuery<IndexQuery> {

    public static final ImmutableList<OrderEntry> NO_ORDER = ImmutableList.of();

    private final String store;
    private final Condition condition;
    private final ImmutableList<OrderEntry> orders;

    private final int hashcode;

    public IndexQuery(String store, Condition condition, ImmutableList<OrderEntry> orders, int limit) {
        super(limit);
        Preconditions.checkNotNull(store);
        Preconditions.checkNotNull(condition);
        Preconditions.checkArgument(orders != null);
        this.condition = condition;
        this.orders = orders;
        this.store = store;

        this.hashcode = Objects.hash(condition, store, orders, limit);
    }

    public IndexQuery(String store, Condition condition, ImmutableList<OrderEntry> orders) {
        this(store, condition, orders, Query.NO_LIMIT);
    }

    public IndexQuery(String store, Condition condition) {
        this(store, condition, NO_ORDER, Query.NO_LIMIT);
    }

    public IndexQuery(String store, Condition condition, int limit) {
        this(store, condition, NO_ORDER, limit);
    }

    public Condition<JanusGraphElement> getCondition() {
        return condition;
    }

    public List<OrderEntry> getOrder() {
        return orders;
    }

    public String getStore() {
        return store;
    }

    @Override
    public IndexQuery setLimit(int limit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexQuery updateLimit(int newLimit) {
        return new IndexQuery(store, condition, orders, newLimit);
    }

    @Override
    public int hashCode() {
        return hashcode;
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
        IndexQuery oth = (IndexQuery) other;
        return store.equals(oth.store) && orders.equals(oth.orders)
                && condition.equals(oth.condition) && getLimit() == oth.getLimit();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[").append(condition.toString()).append("]");
        if (!orders.isEmpty()) {
            b.append(orders);
        }
        if (hasLimit()) {
            b.append("(").append(getLimit()).append(")");
        }
        b.append(":").append(store);
        return b.toString();
    }

    public static class OrderEntry {

        private final String key;
        private final Order order;
        private final Class<?> datatype;

        public OrderEntry(String key, Order order, Class<?> datatype) {
            Preconditions.checkNotNull(key);
            Preconditions.checkNotNull(order);
            Preconditions.checkNotNull(datatype);
            this.key = key;
            this.order = order;
            this.datatype = datatype;
        }

        public String getKey() {
            return key;
        }

        public Order getOrder() {
            return order;
        }

        public Class<?> getDatatype() {
            return datatype;
        }

        @Override
        public int hashCode() {
            return key.hashCode() * 4021 + order.hashCode();
        }

        @Override
        public boolean equals(Object oth) {
            if (this == oth) {
                return true;
            } else if (oth == null) {
                return false;
            } else if (!getClass().isInstance(oth)) {
                return false;
            }
            OrderEntry o = (OrderEntry) oth;
            return key.equals(o.key) && order == o.order;
        }

        @Override
        public String toString() {
            return order + "(" + key + ")";
        }


    }


}
