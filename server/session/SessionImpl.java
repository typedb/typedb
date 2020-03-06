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
 *
 */

package grakn.core.server.session;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.keyspace.AttributeManager;
import grakn.core.kb.keyspace.KeyspaceSchemaCache;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import grakn.core.kb.keyspace.ShardManager;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.TransactionProvider;
import grakn.core.kb.server.exception.SessionException;
import grakn.core.kb.server.exception.TransactionException;
import grakn.core.kb.server.keyspace.Keyspace;

import java.util.function.Consumer;

/**
 * This class represents a Grakn Session.
 * A session is mapped to a single instance of a JanusGraph (they're both bound to a single Keyspace).
 * <p>
 * The role of the Session is to provide multiple independent transactions that can be used by clients to
 * access a specific keyspace.
 * <p>
 * NOTE:
 * - Only 1 transaction per thread can exist.
 * - A transaction cannot be shared between multiple threads, each thread will need to get a new transaction from a session.
 */
public class SessionImpl implements Session {

    // An explicit constraint we enforce that we can have at most 1 tx per thread, so we keep a local reference here
    private final ThreadLocal<Transaction> localOLTPTransactionContainer = new ThreadLocal<>();

    private final Keyspace keyspace;
    private final StandardJanusGraph graph;
    private final KeyspaceSchemaCache keyspaceSchemaCache;
    private final KeyspaceStatistics keyspaceStatistics;
    private final AttributeManager attributeManager;
    private final ShardManager shardManager;
    private Consumer<Session> onClose;

    private boolean isClosed = false;
    private final TransactionProvider transactionProvider;

    /**
     * Instantiates SessionImpl specific for internal use (within Grakn Server),
     * using provided Grakn configuration.
     *
     * @param keyspace to which keyspace the session should be bound to
     */
    // NOTE: this method is used by Grakn KGMS and should be kept public
    public SessionImpl(Keyspace keyspace, TransactionProvider transactionProvider, KeyspaceSchemaCache keyspaceSchemaCache, StandardJanusGraph graph,
                       KeyspaceStatistics keyspaceStatistics, AttributeManager attributeManager, ShardManager shardManager) {
        this.keyspace = keyspace;
        this.transactionProvider = transactionProvider;
        this.graph = graph;
        this.keyspaceSchemaCache = keyspaceSchemaCache;
        this.keyspaceStatistics = keyspaceStatistics;
        this.attributeManager = attributeManager;
        this.shardManager = shardManager;

        Transaction tx = writeTransaction();

        if (!keyspaceHasBeenInitialised(tx)) {
            initialiseMetaConcepts(tx);
        }
        // If keyspace cache is empty, copy schema concept labels in it.
        if (keyspaceSchemaCache.isEmpty()) {
            copySchemaConceptLabelsToKeyspaceCache(tx);
        }

        tx.commit();
    }

    @Override
    public Transaction readTransaction() {
        return transaction(Transaction.Type.READ);
    }

    @Override
    public Transaction writeTransaction() {
        return transaction(Transaction.Type.WRITE);
    }

    private Transaction transaction(Transaction.Type type) {

        // If graph is closed it means the session was already closed
        if (graph.isClosed()) {
            throw new SessionException(ErrorMessage.SESSION_CLOSED.getMessage(keyspace()));
        }

        Transaction localTx = localOLTPTransactionContainer.get();
        // If transaction is already open in current thread throw exception
        if (localTx != null && localTx.isOpen()) throw TransactionException.transactionOpen(localTx);

        Transaction tx = transactionProvider.newTransaction(this);

        tx.open(type);
        localOLTPTransactionContainer.set(tx);

        return tx;
    }

    /**
     * This creates the first meta schema in an empty keyspace which has not been initialised yet
     * Does a lower level operation, so we cast it to the Implementation
     * This means we don't needlessly open the interface up
     *
     * TODO should the `createMetaConcepts` live in SessionImpl rather than TransactionImpl?
     */
    private void initialiseMetaConcepts(Transaction tx) {
        ((TransactionImpl)tx).createMetaConcepts();
    }

    /**
     * Copy schema concepts labels to current KeyspaceCache
     */
    private void copySchemaConceptLabelsToKeyspaceCache(Transaction tx) {
        copyToCache(tx.getMetaConcept());
        copyToCache(tx.getMetaRole());
        copyToCache(tx.getMetaRule());
    }

    /**
     * Copy schema concept and all its subs labels to keyspace cache
     */
    private void copyToCache(SchemaConcept schemaConcept) {
        schemaConcept.subs().forEach(concept -> keyspaceSchemaCache.cacheLabel(concept.label(), concept.labelId()));
    }

    private boolean keyspaceHasBeenInitialised(Transaction tx) {
        return tx.getMetaConcept() != null;
    }


    /**
     * Method used by SessionFactory to register a callback function that has to be triggered when closing current session.
     *
     * @param onClose callback function (this should be used to update the session references in SessionFactory)
     */
    // NOTE: this method is used by Grakn KGMS and should be kept public
    @Override
    public void setOnClose(Consumer<Session> onClose) {
        this.onClose = onClose;
    }

    /**
     * Method used by SessionFactory to invalidate current Session when the keyspace (used by current session) is deleted.
     * This closes current session and local transaction, without invoking callback function.
     */
    @Override
    public void invalidate() {
        isClosed = true;
    }

    /**
     * Close JanusGraph, it will not be possible to create new transactions using current instance of Session.
     * This closes current session and local transaction, invoking callback function if one is set.
     **/
    @Override
    public void close() {
        if (isClosed) {
            return;
        }

        Transaction localTx = localOLTPTransactionContainer.get();
        if (localTx != null) {
            localTx.close(ErrorMessage.SESSION_CLOSED.getMessage(keyspace()));
            localOLTPTransactionContainer.set(null);
        }

        if (this.onClose != null) {
            this.onClose.accept(this);
        }

        isClosed = true;
    }

    @Override
    public Keyspace keyspace() {
        return keyspace;
    }

    @Override
    public KeyspaceStatistics keyspaceStatistics() {
        return keyspaceStatistics;
    }

    @Override
    public AttributeManager attributeManager() {
        return attributeManager;
    }

    @Override
    public ShardManager shardManager() {
        return shardManager;
    }
}
