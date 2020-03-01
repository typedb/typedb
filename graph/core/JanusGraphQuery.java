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

package grakn.core.graph.core;

import grakn.core.graph.graphdb.query.JanusGraphPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.Order;

/**
 * Constructs a query against a mixed index to retrieve all elements (either vertices or edges)
 * that match all conditions.
 * <p>
 * Finding matching elements efficiently using this query mechanism requires that appropriate index structures have
 * been defined for the keys. See JanusGraphManagement for more information
 * on how to define index structures in JanusGraph.
 *
 * @since 0.3.0
 */

public interface JanusGraphQuery<Q extends JanusGraphQuery<Q>> {

    /* ---------------------------------------------------------------
     * Query Specification
     * ---------------------------------------------------------------
     */

    /**
     * The returned element must have a property for the given key that matches the condition according to the
     * specified relation
     *
     * @param key       Key that identifies the property
     * @param predicate Relation between property and condition
     * @return This query
     */
    Q has(String key, JanusGraphPredicate predicate, Object condition);

    Q has(String key);

    Q hasNot(String key);

    Q has(String key, Object value);

    Q hasNot(String key, Object value);

    Q or(Q subQuery);

    <T extends Comparable<?>> Q interval(String key, T startValue, T endValue);

    /**
     * Limits the size of the returned result set
     *
     * @param max The maximum number of results to return
     * @return This query
     */
    Q limit(int max);

    /**
     * Orders the element results of this query according
     * to their property for the given key in the given order (increasing/decreasing).
     *
     * @param key   The key of the properties on which to order
     * @param order the ordering direction
     */
    Q orderBy(String key, Order order);


    /* ---------------------------------------------------------------
     * Query Execution
     * ---------------------------------------------------------------
     */

    /**
     * Returns all vertices that match the conditions.
     */
    Iterable<JanusGraphVertex> vertices();

    /**
     * Returns all edges that match the conditions.
     */
    Iterable<JanusGraphEdge> edges();

    /**
     * Returns all properties that match the conditions
     */
    Iterable<JanusGraphVertexProperty> properties();


}
