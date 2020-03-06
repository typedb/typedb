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

import grakn.core.graph.core.schema.SchemaManager;
import grakn.core.graph.graphdb.relations.RelationIdentifier;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * Transaction defines a transactional context for a JanusGraph. Since JanusGraph is a transactional graph
 * database, all interactions with the graph are mitigated by a Transaction.
 * <p>
 * All vertex and edge retrievals are channeled by a graph transaction which bundles all such retrievals, creations and
 * deletions into one transaction. A graph transaction is analogous to a
 * <a href="https://en.wikipedia.org/wiki/Database_transaction">database transaction</a>.
 * The isolation level and <a href="https://en.wikipedia.org/wiki/ACID">ACID support</a> are configured through the storage
 * backend, meaning whatever level of isolation is supported by the storage backend is mirrored by a graph transaction.
 * <p>
 * A graph transaction supports:
 * <ul>
 * <li>Creating vertices, properties and edges</li>
 * <li>Creating types</li>
 * <li>Index-based retrieval of vertices</li>
 * <li>Querying edges and vertices</li>
 * <li>Aborting and committing transaction</li>
 * </ul>
 */
public interface JanusGraphTransaction extends Graph, SchemaManager {

    /* ---------------------------------------------------------------
     * Modifications
     * ---------------------------------------------------------------
     */

    /**
     * Creates a new vertex in the graph with the vertex label named by the argument.
     *
     * @param vertexLabel the name of the vertex label to use
     * @return a new vertex in the graph created in the context of this transaction
     */
    JanusGraphVertex addVertex(String vertexLabel);

    @Override
    default <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        return null; // This is only implemented in HadoopGraph
    }

    @Override
    default GraphComputer compute() throws IllegalArgumentException {
        return null; // This is only implemented in HadoopGraph
    }

    @Override
    JanusGraphVertex addVertex(Object... objects);


    JanusGraphQuery<? extends JanusGraphQuery> query();

    /**
     * Returns a JanusGraphIndexQuery to query for vertices or edges against the specified indexing backend using
     * the given query string. The query string is analyzed and answered by the underlying storage backend.
     * <p>
     * Note, that using indexQuery may ignore modifications in the current transaction.
     *
     * @param indexName Name of the index to query as configured
     * @param query     Query string
     * @return JanusGraphIndexQuery object to query the index directly
     */
    JanusGraphIndexQuery indexQuery(String indexName, String query);


    JanusGraphMultiVertexQuery<? extends JanusGraphMultiVertexQuery> multiQuery(JanusGraphVertex... vertices);

    @Override
    void close();


    /* ---------------------------------------------------------------
     * Modifications
     * ---------------------------------------------------------------
     */

    /**
     * Creates a new vertex in the graph with the given vertex id and the given vertex label.
     * Note, that an exception is thrown if the vertex id is not a valid JanusGraph vertex id or if a vertex with the given
     * id already exists.
     * <p>
     * A valid JanusGraph vertex ids must be provided. Use IDManager#toVertexId(long)
     * to construct a valid JanusGraph vertex id from a user id, where <code>idManager</code> can be obtained through
     * StandardJanusGraph#getIDManager().
     * <pre>
     * <code>long vertexId = ((StandardJanusGraph) graph).getIDManager().toVertexId(userVertexId);</code>
     * </pre>
     *
     * @param id          vertex id of the vertex to be created
     * @param vertexLabel vertex label for this vertex - can be null if no vertex label should be set.
     * @return New vertex
     */
    JanusGraphVertex addVertex(Long id, VertexLabel vertexLabel);

    /**
     * Retrieves the vertex for the specified id.
     * <p>
     * This method is intended for internal use only. Use org.apache.tinkerpop.gremlin.structure.Graph#vertices(Object...) instead.
     *
     * @param id id of the vertex to retrieve
     * @return vertex with the given id if it exists, else null
     */
    JanusGraphVertex getVertex(long id);


    Iterable<JanusGraphVertex> getVertices(long... ids);

    Iterable<JanusGraphEdge> getEdges(RelationIdentifier... ids);

    /* ---------------------------------------------------------------
     * Closing and admin
     * ---------------------------------------------------------------
     */

    /**
     * Commits and closes the transaction.
     * <p>
     * Will attempt to persist all modifications which may result in exceptions in case of persistence failures or
     * lock contention.
     * <br>
     * The call releases data structures if possible. All element references (e.g. vertex objects) retrieved
     * through this transaction are stale after the transaction closes and should no longer be used.
     */
    void commit();

    /**
     * Aborts and closes the transaction. Will discard all modifications.
     * <p>
     * The call releases data structures if possible. All element references (e.g. vertex objects) retrieved
     * through this transaction are stale after the transaction closes and should no longer be used.
     */
    void rollback();

    /**
     * Checks whether the transaction is still open.
     *
     * @return true, when the transaction is open, else false
     */
    boolean isOpen();

    /**
     * Checks whether the transaction has been closed.
     *
     * @return true, if the transaction has been closed, else false
     */
    boolean isClosed();

    /**
     * Checks whether any changes to the graph database have been made in this transaction.
     * <p>
     * A modification may be an edge or vertex update, addition, or deletion.
     *
     * @return true, if the transaction contains updates, else false.
     */
    boolean hasModifications();


}
