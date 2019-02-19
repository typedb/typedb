/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.server.session;

import brave.ScopedSpan;
import grakn.benchmark.lib.serverinstrumentation.ServerTracingInstrumentation;
import grakn.core.common.config.Config;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.exception.SessionException;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.keyspace.Keyspace;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.util.JanusGraphCleanup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import javax.annotation.CheckReturnValue;

/**
 * This class represents a Grakn Session.
 * A session is mapped to a single instance of a JanusGraph (they're both bound to a single Keyspace):
 * opening a session will open a new JanusGraph, closing a session will close the graph.
 *
 * The role of the Session is to provide multiple independent transactions that can be used by clients to
 * access a specific keyspace.
 *
 * NOTE:
 *  - Only 1 transaction per thread can exist.
 *  - A transaction cannot be shared between multiple threads, each thread will need to get a new transaction from a session.
 */
public class SessionImpl implements Session {

    private final HadoopGraphFactory hadoopGraphFactory;

    // Session can have at most 1 transaction per thread, so we keep a local reference here
    private final ThreadLocal<TransactionOLTP> localOLTPTransactionContainer = new ThreadLocal<>();

    private final Keyspace keyspace;
    private final Config config;
    private final JanusGraph graph;


    /**
     * Instantiates {@link SessionImpl} specific for internal use (within Grakn Server),
     * using provided Grakn configuration
     *
     * @param keyspace to which keyspace the session should be bound to
     * @param config   config to be used. If null is supplied, it will be created
     */
    public SessionImpl(Keyspace keyspace, Config config) {
        this.keyspace = keyspace;
        this.config = config;
        // Only save a reference to the factory rather than opening an Hadoop graph immediately because that can be
        // be an expensive operation TODO: refactor in the future
        this.hadoopGraphFactory = new HadoopGraphFactory(this);
        // Open Janus Graph
        this.graph = JanusGraphFactory.openGraph(this);
    }


    @Override
    public TransactionOLTP transaction(Transaction.Type type) {

        ScopedSpan span = null;
        if (ServerTracingInstrumentation.tracingActive()) span = ServerTracingInstrumentation.createScopedChildSpan("SessionImpl.transaction");

        // If graph is closed it means the session was already closed
        if (graph.isClosed()) throw new SessionException(ErrorMessage.SESSION_CLOSED.getMessage(keyspace()));

        if (span != null) span.annotate("Getting local thread to see if need to throw exception");
        TransactionOLTP localTx = localOLTPTransactionContainer.get();
        // If transaction is already open in current thread throw exception
        if (localTx != null && !localTx.isClosed()) throw TransactionException.transactionOpen(localTx);

        if (span != null) span.annotate("Getting new tx");
        // We are passing the graph to Transaction because there is the need to access graph tinkerpop traversal
        TransactionOLTP tx = new TransactionOLTP(this, graph);

        if (span != null) span.annotate("Opening tx with type");
        tx.open(type);

        if (span != null) span.annotate("Saving tx to local container");

        localOLTPTransactionContainer.set(tx);

        if (span != null) span.finish();
        return tx;
    }

    public void clearGraph() {
        try {
            JanusGraphCleanup.clear(graph);
        } catch (BackendException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get a new or existing TransactionOLAP.
     *
     * @return A new or existing Grakn graph computer
     * @see TransactionOLAP
     */
    @CheckReturnValue
    public TransactionOLAP transactionOLAP() {
        return new TransactionOLAP(hadoopGraphFactory.getGraph());
    }

    /**
     * Close JanusGraph, it will not be possible to create new transactions using current instance of Session.
     * If there is a transaction open, close it before closing the graph.
     * @throws TransactionException
     */
    @Override
    public void close() {
        TransactionOLTP localTx = localOLTPTransactionContainer.get();
        if (localTx != null) {
            localTx.close(ErrorMessage.SESSION_CLOSED.getMessage(keyspace()));
            localOLTPTransactionContainer.set(null);
        }

        ((StandardJanusGraph) graph).getOpenTransactions().forEach(org.janusgraph.core.Transaction::close);
        graph.close();
    }

    @Override
    public Keyspace keyspace() {
        return keyspace;
    }

    /**
     * The config options of this {@link Session} which were passed in at the time of construction
     *
     * @return The config options of this {@link Session}
     */
    public Config config() {
        return config;
    }
}
