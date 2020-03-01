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

import grakn.core.graph.core.schema.Parameter;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.stream.Stream;

/**
 * A GraphQuery that queries for graph elements directly against a particular indexing backend and hence allows this
 * query mechanism to exploit the full range of features and functionality of the indexing backend.
 * However, the results returned by this query will not be adjusted to the modifications in a transaction. If there
 * are no changes in a transaction, this won't matter. If there are, the results of this query may not be consistent
 * with the transactional state.
 */
public interface JanusGraphIndexQuery {

    /**
     * Specifies the maximum number of elements to return
     */
    JanusGraphIndexQuery limit(int limit);

    /**
     * Specifies the offset of the query. Query results will be retrieved starting at the given offset.
     */
    JanusGraphIndexQuery offset(int offset);

    /**
     * Orders the element results of this query according
     * to their property for the given key in the given order (increasing/decreasing).
     */
    JanusGraphIndexQuery orderBy(String key, Order order);

    /**
     * Adds the given parameter to the list of parameters of this query.
     * Parameters are passed right through to the indexing backend to modify the query behavior.
     */
    JanusGraphIndexQuery addParameter(Parameter para);

    /**
     * Adds the given parameters to the list of parameters of this query.
     * Parameters are passed right through to the indexing backend to modify the query behavior.
     */
    JanusGraphIndexQuery addParameters(Iterable<Parameter> paras);

    /**
     * Adds the given parameters to the list of parameters of this query.
     * Parameters are passed right through to the indexing backend to modify the query behavior.
     */
    JanusGraphIndexQuery addParameters(Parameter... paras);

    /**
     * Sets the element identifier string that is used by this query builder as the token to identifier key references
     * in the query string.
     * <p>
     * For example, in the query 'v.name: Tom' the element identifier is 'v.'
     *
     * @param identifier The element identifier which must not be blank
     * @return This query builder
     */
    JanusGraphIndexQuery setElementIdentifier(String identifier);

    /**
     * Returns all vertices that match the query in the indexing backend.
     *
     * @deprecated use #vertexStream() instead.
     */
    @Deprecated
    Iterable<Result<JanusGraphVertex>> vertices();

    /**
     * Returns all vertices that match the query in the indexing backend.
     */
    Stream<Result<JanusGraphVertex>> vertexStream();

    /**
     * Returns all edges that match the query in the indexing backend.
     *
     * @deprecated use #edgeStream() instead.
     */
    @Deprecated
    Iterable<Result<JanusGraphEdge>> edges();

    /**
     * Returns all edges that match the query in the indexing backend.
     */
    Stream<Result<JanusGraphEdge>> edgeStream();

    /**
     * Returns all properties that match the query in the indexing backend.
     *
     * @deprecated use #propertyStream() instead.
     */
    @Deprecated
    Iterable<Result<JanusGraphVertexProperty>> properties();

    /**
     * Returns all properties that match the query in the indexing backend.
     */
    Stream<Result<JanusGraphVertexProperty>> propertyStream();

    /**
     * Returns total vertices that match the query in the indexing backend ignoring limit and offset.
     */
    Long vertexTotals();

    /**
     * Returns total edges that match the query in the indexing backend ignoring limit and offset.
     */
    Long edgeTotals();

    /**
     * Returns total properties that match the query in the indexing backend ignoring limit and offset.
     */
    Long propertyTotals();

    /**
     * Container of a query result with its score.
     *
     * @param <V>
     */
    interface Result<V extends Element> {

        /**
         * Returns the element that matches the query
         */
        V getElement();

        /**
         * Returns the score of the result with respect to the query (if available)
         */
        double getScore();

    }


}
