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

package grakn.core.server.session;

import grakn.core.common.config.Config;
import grakn.core.common.config.ConfigKey;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import grakn.core.kb.keyspace.AttributeManager;
import grakn.core.kb.keyspace.KeyspaceSchemaCache;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import grakn.core.kb.keyspace.ShardManager;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.TransactionProvider;
import grakn.core.kb.server.keyspace.Keyspace;
import grakn.core.keyspace.AttributeManagerImpl;
import grakn.core.keyspace.KeyspaceStatisticsImpl;
import grakn.core.keyspace.ShardManagerImpl;
import grakn.core.server.util.LockManager;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Grakn Server's internal SessionImpl Factory
 * All components should use this factory so that every time a session to a new keyspace gets created
 * it is possible to also update the Keyspace Store (which tracks all existing keyspaces).
 */
public class SessionFactory {

    // Keep visibility to protected as this is used by KGMS
    protected final JanusGraphFactory janusGraphFactory;
    // Keep visibility to protected as this is used by KGMS
    protected final HadoopGraphFactory hadoopGraphFactory;
    protected Config config;
    // Keep visibility to protected as this is used by KGMS
    protected final LockManager lockManager;

    private final Map<Keyspace, SharedKeyspaceData> sharedKeyspaceDataMap;

    public SessionFactory(LockManager lockManager, JanusGraphFactory janusGraphFactory, HadoopGraphFactory hadoopGraphFactory, Config config) {
        this.janusGraphFactory = janusGraphFactory;
        this.hadoopGraphFactory = hadoopGraphFactory;
        this.lockManager = lockManager;
        this.config = config;
        this.sharedKeyspaceDataMap = new HashMap<>();
    }

    /**
     * Creates a Session needed to open Transactions.
     * This will open a new graph or retrieve one from shared cache.
     *
     * @param keyspace The keyspace of the Session to retrieve
     * @return a new Session connecting to the provided keyspace
     */
    public Session session(Keyspace keyspace) {
        SharedKeyspaceData cacheContainer;
        StandardJanusGraph graph;
        KeyspaceSchemaCache cache;
        KeyspaceStatistics keyspaceStatistics;
        AttributeManager attributeManager;
        ShardManager shardManager;
        ReadWriteLock graphLock;
        HadoopGraph hadoopGraph;

        Lock lock = lockManager.getLock(keyspace.name());
        lock.lock();

        try {
            // If keyspace reference already in cache, retrieve open graph and keyspace cache
            if (sharedKeyspaceDataMap.containsKey(keyspace)) {
                cacheContainer = sharedKeyspaceDataMap.get(keyspace);
                graph = cacheContainer.graph();
                cache = cacheContainer.cache();
                keyspaceStatistics = cacheContainer.keyspaceStatistics();
                attributeManager = cacheContainer.attributeManager();
                shardManager = cacheContainer.shardManager();
                graphLock = cacheContainer.graphLock();
                hadoopGraph = cacheContainer.hadoopGraph();

            } else { // If keyspace reference not cached, put keyspace in keyspace manager, open new graph and instantiate new keyspace cache
                graph = janusGraphFactory.openGraph(keyspace.name());
                hadoopGraph = hadoopGraphFactory.getGraph(keyspace);
                cache = new KeyspaceSchemaCache();
                keyspaceStatistics = new KeyspaceStatisticsImpl();
                attributeManager = new AttributeManagerImpl();
                shardManager = new ShardManagerImpl();
                graphLock = new ReentrantReadWriteLock();
                cacheContainer = new SharedKeyspaceData(cache, graph, keyspaceStatistics, attributeManager, shardManager, graphLock, hadoopGraph);
                sharedKeyspaceDataMap.put(keyspace, cacheContainer);
            }

            long typeShardThreshold = config.getProperty(ConfigKey.TYPE_SHARD_THRESHOLD);
            TransactionProvider transactionProvider = new TransactionProviderImpl(graph, hadoopGraph, cache, keyspaceStatistics, attributeManager, graphLock, typeShardThreshold);
            Session session = new SessionImpl(keyspace, transactionProvider, cache, graph, keyspaceStatistics, attributeManager, shardManager);
            session.setOnClose(this::onSessionClose);
            cacheContainer.addSessionReference(session);
            return session;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Invoked when user deletes a keyspace.
     * Remove keyspace reference from internal cache, closes graph associated to it and
     * invalidates all the open sessions.
     *
     * @param keyspace keyspace that is being deleted
     */
    public void deleteKeyspace(Keyspace keyspace) {
        Lock lock = lockManager.getLock(keyspace.name());
        lock.lock();
        try {
            if (sharedKeyspaceDataMap.containsKey(keyspace)) {
                SharedKeyspaceData container = sharedKeyspaceDataMap.remove(keyspace);
                container.graph().close();
                container.hadoopGraph().close();
                container.invalidateSessions();
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * Callback function invoked by Session when it gets closed.
     * This access the sharedKeyspaceDataMap to remove the reference of closed session.
     *
     * @param session Session that is being closed
     */
    // Keep visibility to protected as this is used by KGMS
    protected void onSessionClose(Session session) {
        Lock lock = lockManager.getLock(session.keyspace().name());
        lock.lock();
        try {
            if (sharedKeyspaceDataMap.containsKey(session.keyspace())) {
                SharedKeyspaceData cacheContainer = sharedKeyspaceDataMap.get(session.keyspace());
                cacheContainer.removeSessionReference(session);
                // If there are no more sessions associated to current keyspace,
                // close graph and remove reference from cache.
                if (cacheContainer.referenceCount() == 0) {
                    cacheContainer.graph().close();
                    cacheContainer.hadoopGraph().close();
                    sharedKeyspaceDataMap.remove(session.keyspace());
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Helper class used to hold in memory a reference to a graph together with its schema cache
     * and a reference to all sessions open to the graph.
     */
    // Keep visibility to protected as this is used by KGMS
    protected class SharedKeyspaceData {

        private final KeyspaceSchemaCache keyspaceSchemaCache;
        // Graph is cached here because concurrently created sessions don't see writes to JanusGraph DB cache
        private final StandardJanusGraph graph;
        private final HadoopGraph hadoopGraph;
        // Keep track of sessions so that if a user deletes a keyspace we make sure to invalidate all associated sessions
        private final List<Session> sessions;

        // Shared keyspace statistics
        private final KeyspaceStatistics keyspaceStatistics;

        // Map<AttributeIndex, ConceptId> used to map an attribute index to a unique id
        // so that concurrent transactions can merge the same attribute indexes using a unique id
        private final AttributeManager attributeManager;

        private final ShardManager shardManager;

        private final ReadWriteLock graphLock;

        // Keep visibility to public as this is used by KGMS
        public SharedKeyspaceData(KeyspaceSchemaCache keyspaceSchemaCache, StandardJanusGraph graph, KeyspaceStatistics keyspaceStatistics,
                                  AttributeManager attributeManager, ShardManager shardManager, ReadWriteLock graphLock, HadoopGraph hadoopGraph) {
            this.keyspaceSchemaCache = keyspaceSchemaCache;
            this.graph = graph;
            this.hadoopGraph = hadoopGraph;
            this.sessions = new ArrayList<>();
            this.keyspaceStatistics = keyspaceStatistics;
            this.attributeManager = attributeManager;
            this.shardManager = shardManager;
            this.graphLock = graphLock;
        }

        // Keep visibility to public as this is used by KGMS
        public ReadWriteLock graphLock() {
            return graphLock;
        }

        // Keep visibility to public as this is used by KGMS
        public KeyspaceSchemaCache cache() {
            return keyspaceSchemaCache;
        }

        // Keep visibility to public as this is used by KGMS
        public int referenceCount() {
            return sessions.size();
        }

        // Keep visibility to public as this is used by KGMS
        public void addSessionReference(Session session) {
            sessions.add(session);
        }

        // Keep visibility to public as this is used by KGMS
        public void removeSessionReference(Session session) {
            sessions.remove(session);
        }

        void invalidateSessions() {
            sessions.forEach(Session::invalidate);
        }

        // Keep visibility to public as this is used by KGMS
        public StandardJanusGraph graph() {
            return graph;
        }

        // Keep visibility to public as this is used by KGMS
        public KeyspaceStatistics keyspaceStatistics() {
            return keyspaceStatistics;
        }

        // Keep visibility to public as this is used by KGMS
        public AttributeManager attributeManager() {
            return attributeManager;
        }

        public ShardManager shardManager(){ return shardManager;}

        // Keep visibility to public as this is used by KGMS
        public HadoopGraph hadoopGraph() {
            return hadoopGraph;
        }

    }

}