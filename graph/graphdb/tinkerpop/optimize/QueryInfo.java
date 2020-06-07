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

package grakn.core.graph.graphdb.tinkerpop.optimize;

import grakn.core.graph.graphdb.tinkerpop.optimize.HasStepFolder.OrderEntry;

import java.util.List;
import java.util.Objects;

public class QueryInfo {

    private final List<OrderEntry> orders;

    private Integer lowLimit;

    private Integer highLimit;

    public QueryInfo(List<OrderEntry> orders, Integer lowLimit, Integer highLimit) {
        this.orders = orders;
        this.lowLimit = lowLimit;
        this.highLimit = highLimit;
    }

    public List<OrderEntry> getOrders() {
        return orders;
    }

    public Integer getLowLimit() {
        return lowLimit;
    }

    public Integer getHighLimit() {
        return highLimit;
    }

    public QueryInfo setLowLimit(Integer lowLimit) {
        this.lowLimit = lowLimit;
        return this;
    }

    public QueryInfo setHighLimit(Integer highLimit) {
        this.highLimit = highLimit;
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(orders, lowLimit, highLimit);
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
        QueryInfo oth = (QueryInfo) other;
        return Objects.equals(orders, oth.orders) && Objects.equals(lowLimit, oth.lowLimit) && highLimit.equals(oth.highLimit);
    }
}
