/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

import grakn.core.common.config.Config;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.concept.impl.ConceptManagerImpl;
import grakn.core.concept.impl.ConceptObserver;
import grakn.core.concept.structure.ElementFactory;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.server.AttributeManager;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.ShardManager;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.TransactionAnalytics;
import grakn.core.kb.server.cache.KeyspaceSchemaCache;
import grakn.core.kb.server.exception.SessionException;
import grakn.core.kb.server.exception.TransactionException;
import grakn.core.kb.server.keyspace.Keyspace;
import grakn.core.kb.server.statistics.KeyspaceStatistics;
import grakn.core.kb.server.statistics.UncomittedStatisticsDelta;
import grakn.core.server.cache.CacheProviderImpl;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;
import javax.annotation.CheckReturnValue;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.graph.graphdb.database.StandardJanusGraph;

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

    private final HadoopGraph hadoopGraph;

    // Session can have at most 1 transaction per thread, so we keep a local reference here
    private final ThreadLocal<Transaction> localOLTPTransactionContainer = new ThreadLocal<>();

    private final Keyspace keyspace;
    private final Config config;
    private final StandardJanusGraph graph;
    private final KeyspaceSchemaCache keyspaceSchemaCache;
    private final KeyspaceStatistics keyspaceStatistics;
    private final AttributeManager attributeManager;
    private final ShardManager shardManager;
    private final ReadWriteLock graphLock;
    private Consumer<Session> onClose;

    private boolean isClosed = false;

    /**
     * Instantiates {@link SessionImpl} specific for internal use (within Grakn Server),
     * using provided Grakn configuration.
     *
     * @param keyspace to which keyspace the session should be bound to
     * @param config   config to be used.
     */
    public SessionImpl(Keyspace keyspace, Config config, KeyspaceSchemaCache keyspaceSchemaCache, StandardJanusGraph graph, KeyspaceStatistics keyspaceStatistics,
                       AttributeManager attributeManager, ShardManager shardManager, ReadWriteLock graphLock) {
        this(keyspace, config, keyspaceSchemaCache, graph, null, keyspaceStatistics, attributeManager, shardManager, graphLock);
    }

    /**
     * Instantiates {@link SessionImpl} specific for internal use (within Grakn Server),
     * using provided Grakn configuration.
     *
     * @param keyspace to which keyspace the session should be bound to
     * @param config   config to be used.
     */
    // NOTE: this method is used by Grakn KGMS and should be kept public
     public SessionImpl(Keyspace keyspace, Config config, KeyspaceSchemaCache keyspaceSchemaCache, StandardJanusGraph graph,
                        HadoopGraph hadoopGraph, KeyspaceStatistics keyspaceStatistics,
                        AttributeManager attributeManager, ShardManager shardManager, ReadWriteLock graphLock) {
        this.keyspace = keyspace;
        this.config = config;
        this.hadoopGraph = hadoopGraph;
        this.graph = graph;

        this.keyspaceSchemaCache = keyspaceSchemaCache;
        this.keyspaceStatistics = keyspaceStatistics;
        this.attributeManager = attributeManager;
        this.shardManager = shardManager;
        this.graphLock = graphLock;

        TransactionOLTP tx = this.transaction(Transaction.Type.WRITE);

        if (!keyspaceHasBeenInitialised(tx)) {
            initialiseMetaConcepts(tx);
        }
        // If keyspace cache is empty, copy schema concept labels in it.
        if (keyspaceSchemaCache.isEmpty()) {
            copySchemaConceptLabelsToKeyspaceCache(tx);
        }

        tx.commit();
    }

    ReadWriteLock graphLock() {
        return graphLock;
    }

    @Override
    public Transaction readTransaction() {
        return transaction(Transaction.Type.READ);
    }

    @Override
    public Transaction writeTransaction() {
        return transaction(Transaction.Type.WRITE);
    }

    TransactionOLTP transaction(Transaction.Type type) {

        // If graph is closed it means the session was already closed
        if (graph.isClosed()) {
            throw new SessionException(ErrorMessage.SESSION_CLOSED.getMessage(keyspace()));
        }

        Transaction localTx = localOLTPTransactionContainer.get();
        // If transaction is already open in current thread throw exception
        if (localTx != null && localTx.isOpen()) throw TransactionException.transactionOpen(localTx);

        // janus elements
        JanusGraphTransaction janusGraphTransaction = graph.newThreadBoundTransaction();
        ElementFactory elementFactory = new ElementFactory(janusGraphTransaction);

        // caches
        CacheProviderImpl cacheProvider = new CacheProviderImpl(keyspaceSchemaCache);
        UncomittedStatisticsDelta statisticsDelta = new UncomittedStatisticsDelta();
        ConceptObserver conceptObserver = new ConceptObserver(cacheProvider, statisticsDelta, attributeManager(), janusGraphTransaction.toString());

        // Grakn elements
        ConceptManagerImpl conceptManager = new ConceptManagerImpl(elementFactory, cacheProvider.getTransactionCache(), conceptObserver, attributeManager());

        TransactionOLTP tx = new TransactionOLTP(this, janusGraphTransaction, conceptManager, cacheProvider, statisticsDelta);

        tx.open(type);
        localOLTPTransactionContainer.set(tx);

        return tx;
    }

    /**
     * This creates the first meta schema in an empty keyspace which has not been initialised yet
     */
    private void initialiseMetaConcepts(TransactionOLTP tx) {
        tx.createMetaConcepts();
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
     * Get a new or existing TransactionOLAP.
     *
     * @return A new or existing Grakn graph computer
     * @see TransactionOLAP
     */
    @Override
    @CheckReturnValue
    public TransactionAnalytics transactionOLAP() {
        return new TransactionOLAP(hadoopGraph);
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
        Transaction localTx = localOLTPTransactionContainer.get();
        if (localTx != null) {
            localTx.close(ErrorMessage.SESSION_CLOSED.getMessage(keyspace()));
            localOLTPTransactionContainer.set(null);
        }
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

    /**
     * The config options of this {@link SessionImpl} which were passed in at the time of construction
     *
     * @return The config options of this {@link SessionImpl}
     */
    public Config config() {
        return config;
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
