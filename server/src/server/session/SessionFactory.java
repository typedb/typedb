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

import grakn.core.common.config.Config;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.keyspace.Keyspace;
import grakn.core.server.keyspace.KeyspaceManager;
import grakn.core.server.session.cache.KeyspaceCache;
import grakn.core.server.util.LockManager;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

/**
 * Grakn Server's internal {@link SessionImpl} Factory
 * All components should use this factory so that every time a session to a new keyspace gets created
 * it is possible to also update the Keyspace Store (which tracks all existing keyspaces).
 */
public class SessionFactory {
    private final JanusGraphFactory janusGraphFactory;
    private final KeyspaceManager keyspaceManager;
    private Config config;
    private final LockManager lockManager;

    private final ConcurrentHashMap<Keyspace, KeyspaceCacheContainer> keyspaceCacheMap;

    public SessionFactory(LockManager lockManager, JanusGraphFactory janusGraphFactory, KeyspaceManager keyspaceManager, Config config) {
        this.janusGraphFactory = janusGraphFactory;
        this.lockManager = lockManager;
        this.keyspaceManager = keyspaceManager;
        this.config = config;
        this.keyspaceCacheMap = new ConcurrentHashMap<>();
    }

    /**
     * Retrieves the {@link Session} needed to open the {@link Transaction}.
     * This will open a new one {@link Session} if it hasn't been opened before
     *
     * @param keyspace The {@link Keyspace} of the {@link Session} to retrieve
     * @return a new or existing {@link Session} connecting to the provided {@link Keyspace}
     */
    public SessionImpl session(Keyspace keyspace) {
        JanusGraph graph;
        KeyspaceCache cache;
        KeyspaceCacheContainer cacheContainer;

        Lock lock = lockManager.getLock(getLockingKey(keyspace));
        lock.lock();

        try {
            if (keyspaceCacheMap.contains(keyspace)) {
                graph = keyspaceCacheMap.get(keyspace).graph();
                cache = keyspaceCacheMap.get(keyspace).cache();
                cacheContainer = keyspaceCacheMap.get(keyspace);
            } else {
                keyspaceManager.putKeyspace(keyspace);
                graph = janusGraphFactory.openGraph(keyspace.getName());
                cache = new KeyspaceCache(config);
                cacheContainer = new KeyspaceCacheContainer(cache, graph);
                keyspaceCacheMap.put(keyspace, cacheContainer);
            }
            cacheContainer.incrementReferenceCount();
            SessionImpl session = new SessionImpl(keyspace, config, cache, graph, (sessionToClose) -> onSessionClose(sessionToClose));
            cacheContainer.addSessionReference(session);

        } finally {
            lock.unlock();
        }
    }

    public void deleteKeyspace(Keyspace keyspace) {
        Lock lock = lockManager.getLock(getLockingKey(keyspace));
        lock.lock();
        try {
            if (keyspaceCacheMap.contains(keyspace)) {
                KeyspaceCacheContainer container = keyspaceCacheMap.remove(keyspace);
                container.graph().close();
                container.closeSessions();
            }
        } finally {
            lock.unlock();
        }
    }


    private void onSessionClose(SessionImpl session) {
        Lock lock = lockManager.getLock(getLockingKey(session.keyspace()));
        lock.lock();
        try {
            KeyspaceCacheContainer mapCacheContainer = keyspaceCacheMap.get(session.keyspace());

            mapCacheContainer.decrementReferenceCount();
            if (mapCacheContainer.referenceCount() == 0) {
                //Time to close and delete reference in cache
                JanusGraph graph = mapCacheContainer.graph();
                ((StandardJanusGraph) graph).getOpenTransactions().forEach(org.janusgraph.core.Transaction::close);
                graph.close();
                keyspaceCacheMap.remove(session.keyspace());
            } else {
                mapCacheContainer.removeSessionReference();
            }

        } finally {
            lock.unlock();
        }
    }

    private static String getLockingKey(Keyspace keyspace) {
        return "/keyspace-lock/" + keyspace.getName();
    }


    private class KeyspaceCacheContainer {

        private KeyspaceCache keyspaceCache;
        // Graph is cached here because concurrently created sessions don't see writes to JanusGraph DB cache
        private JanusGraph graph;
        private int references;
        // Keep track of sessions so that if a user deletes a keyspace we make sure to close all associated sessions
        private List<SessionImpl> sessions;

        public KeyspaceCacheContainer(KeyspaceCache keyspaceCache, JanusGraph graph) {
            this.keyspaceCache = keyspaceCache;
            this.graph = graph;
            references = 0;
            sessions = new ArrayList<>();
        }

        public KeyspaceCache cache() {
            return keyspaceCache;
        }

        void incrementReferenceCount() {
            references++;
        }

        void decrementReferenceCount() {
            references--;
        }

        int referenceCount() {
            return references;
        }

        void addSessionReference(SessionImpl session) {
            sessions.add(session);
        }

        void removeSessionReference(SessionImpl session) {
            sessions.remove(session);
        }

        void closeSessions() {
            sessions.forEach(SessionImpl::invalidate);
        }

        public JanusGraph graph() {
            return graph;
        }


    }

}