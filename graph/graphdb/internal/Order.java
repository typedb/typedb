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

package grakn.core.graph.graphdb.internal;

/**
 * Constants to specify the ordering of a result set in queries.
 *
 */

public enum Order {

    /**
     * Increasing
     */
    ASC,
    /**
     * Decreasing
     */
    DESC;

    /**
     * Modulates the result of a Comparable#compareTo(Object) execution for this specific
     * order, i.e. it negates the result if the order is #DESC.
     */
    public int modulateNaturalOrder(int compare) {
        switch (this) {
            case ASC:
                return compare;
            case DESC:
                return -compare;
            default:
                throw new AssertionError("Unrecognized order: " + this);
        }
    }

    /**
     * The default order when none is specified
     */
    public static final Order DEFAULT = ASC;

    public org.apache.tinkerpop.gremlin.process.traversal.Order getTP() {
        switch (this) {
            case ASC :return org.apache.tinkerpop.gremlin.process.traversal.Order.asc;
            case DESC: return org.apache.tinkerpop.gremlin.process.traversal.Order.desc;
            default: throw new AssertionError();
        }
    }

    public static Order convert(org.apache.tinkerpop.gremlin.process.traversal.Order order) {
        switch(order) {
            case asc: case incr: return ASC;
            case desc: case decr: return DESC;
            default: throw new AssertionError();
        }
    }

}
