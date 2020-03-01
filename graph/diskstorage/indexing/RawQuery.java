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
import grakn.core.graph.core.schema.Parameter;
import grakn.core.graph.graphdb.query.BaseQuery;
import org.apache.commons.lang.StringUtils;


public class RawQuery extends BaseQuery {

    private final String store;
    private final String query;
    private final Parameter[] parameters;
    private ImmutableList<IndexQuery.OrderEntry> orders;
    private int offset;

    public RawQuery(String store, String query, Parameter[] parameters) {
        this(store, query, ImmutableList.of(), parameters);
    }

    public RawQuery(String store, String query, ImmutableList<IndexQuery.OrderEntry> orders, Parameter[] parameters) {
        Preconditions.checkNotNull(store);
        Preconditions.checkArgument(StringUtils.isNotBlank(query));
        Preconditions.checkNotNull(orders);
        Preconditions.checkNotNull(parameters);
        this.store = store;
        this.query = query;
        this.parameters = parameters;
        this.offset = 0;
        this.orders = orders;
    }

    public RawQuery setOffset(int offset) {
        Preconditions.checkArgument(offset>=0,"Invalid offset: %s",offset);
        this.offset=offset;
        return this;
    }

    @Override
    public RawQuery setLimit(int limit) {
        super.setLimit(limit);
        return this;
    }

    public int getOffset() {
        return offset;
    }

    public String getStore() {
        return store;
    }

    public String getQuery() {
        return query;
    }

    public ImmutableList<IndexQuery.OrderEntry> getOrders() {
        return orders;
    }

    public Parameter[] getParameters() {
        return parameters;
    }

    public static class Result<O> {

        private final O result;
        private final double score;


        public Result(O result, double score) {
            this.result = result;
            this.score = score;
        }

        public O getResult() {
            return result;
        }

        public double getScore() {
            return score;
        }
    }

}
