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

import com.google.common.collect.Iterables;
import grakn.core.graph.graphdb.query.JanusGraphPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * A JanusGraphVertexQuery is a VertexQuery executed for a single vertex.
 * <p>
 * Calling JanusGraphVertex#query() builds such a query against the vertex
 * this method is called on. This query builder provides the methods to specify which incident edges or
 * properties to query for.
 *
 * see BaseVertexQuery
 */
public interface JanusGraphVertexQuery<Q extends JanusGraphVertexQuery<Q>> extends BaseVertexQuery<Q> {

    /* ---------------------------------------------------------------
     * Query Specification (overwrite to merge BaseVertexQuery with Blueprint's VertexQuery)
     * ---------------------------------------------------------------
     */

    @Override
    Q adjacent(Vertex vertex);

    @Override
    Q types(String... type);

    @Override
    Q types(RelationType... type);

    @Override
    Q labels(String... labels);

    @Override
    Q keys(String... keys);

    @Override
    Q direction(Direction d);

    @Override
    Q has(String type, Object value);

    @Override
    Q has(String key);

    @Override
    Q hasNot(String key);

    @Override
    Q hasNot(String key, Object value);

    @Override
    Q has(String key, JanusGraphPredicate predicate, Object value);

    @Override
    <T extends Comparable<?>> Q interval(String key, T start, T end);

    @Override
    Q limit(int limit);

    @Override
    Q orderBy(String key, Order order);


    /* ---------------------------------------------------------------
     * Query execution
     * ---------------------------------------------------------------
     */

    /**
     * Returns an iterable over all incident edges that match this query
     *
     * @return Iterable over all incident edges that match this query
     */
    Iterable<JanusGraphEdge> edges();


    Iterable<JanusGraphVertex> vertices();

    /**
     * Returns an iterable over all incident properties that match this query
     *
     * @return Iterable over all incident properties that match this query
     */
    Iterable<JanusGraphVertexProperty> properties();

    /**
     * Returns an iterable over all incident relations that match this query
     *
     * @return Iterable over all incident relations that match this query
     */
    Iterable<JanusGraphRelation> relations();

    /**
     * Returns the number of relations that match this query
     *
     * @return Number of relations that match this query
     */
    default long count() {
        return Iterables.size(relations());
    }

    /**
     * Returns the number of edges that match this query
     *
     * @return Number of edges that match this query
     */
    default long edgeCount() {
        return vertexIds().size();
    }

    /**
     * Returns the number of properties that match this query
     *
     * @return Number of properties that match this query
     */
    default long propertyCount() {
        return Iterables.size(properties());
    }

    /**
     * Retrieves all vertices connected to this query's base vertex by edges
     * matching the conditions defined in this query.
     * <p>
     * The query engine will determine the most efficient way to retrieve the vertices that match this query.
     *
     * @return A list of all vertices connected to this query's base vertex by matching edges
     */
    VertexList vertexIds();


}
