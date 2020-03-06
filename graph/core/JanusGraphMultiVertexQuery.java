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
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Map;

/**
 * A MultiVertexQuery is identical to a JanusGraphVertexQuery but executed against a set of vertices simultaneously.
 * In other words, JanusGraphMultiVertexQuery allows identical JanusGraphVertexQuery executed against a non-trivial set
 * of vertices to be executed in one batch which can significantly reduce the query latency.
 * <p>
 * The query specification methods are identical to JanusGraphVertexQuery. The result set method return Maps from the specified
 * set of anchor vertices to their respective individual result sets.
 * <p>
 * Call JanusGraphTransaction#multiQuery() to construct a multi query in the enclosing transaction.
 * <p>
 * Note, that the #limit(int) constraint applies to each individual result set.
 *
 * see JanusGraphVertexQuery

 */
public interface JanusGraphMultiVertexQuery<Q extends JanusGraphMultiVertexQuery<Q>> extends BaseVertexQuery<Q> {

   /* ---------------------------------------------------------------
    * Query Specification
    * ---------------------------------------------------------------
    */

    /**
     * Adds the given vertex to the set of vertices against which to execute this query.
     *
     * @return this query builder
     */
    JanusGraphMultiVertexQuery addVertex(Vertex vertex);

    /**
     * Adds the given collection of vertices to the set of vertices against which to execute this query.
     *
     * @return this query builder
     */
    JanusGraphMultiVertexQuery addAllVertices(Collection<? extends Vertex> vertices);


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
     * Returns an iterable over all incident edges that match this query for each vertex
     *
     * @return Iterable over all incident edges that match this query for each vertex
     */
    Map<JanusGraphVertex, Iterable<JanusGraphEdge>> edges();

    /**
     * Returns an iterable over all incident properties that match this query for each vertex
     *
     * @return Iterable over all incident properties that match this query for each vertex
     */
    Map<JanusGraphVertex, Iterable<JanusGraphVertexProperty>> properties();

    /**
     * Makes a call to properties to pre-fetch the properties into the vertex cache
     */
    void preFetch();

    /**
     * Returns an iterable over all incident relations that match this query for each vertex
     *
     * @return Iterable over all incident relations that match this query for each vertex
     */
    Map<JanusGraphVertex, Iterable<JanusGraphRelation>> relations();

    /**
     * Retrieves all vertices connected to each of the query's base vertices by edges
     * matching the conditions defined in this query.
     * <p>
     *
     * @return An iterable of all vertices connected to each of the query's central vertices by matching edges
     */
    Map<JanusGraphVertex, Iterable<JanusGraphVertex>> vertices();

    /**
     * Retrieves all vertices connected to each of the query's central vertices by edges
     * matching the conditions defined in this query.
     * <p>
     * The query engine will determine the most efficient way to retrieve the vertices that match this query.
     *
     * @return A list of all vertices' ids connected to each of the query's central vertex by matching edges
     */
    Map<JanusGraphVertex, VertexList> vertexIds();

}
