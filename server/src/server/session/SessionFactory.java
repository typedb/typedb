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

import grakn.core.api.Keyspace;
import grakn.core.api.Session;
import grakn.core.api.Transaction;
import grakn.core.common.config.Config;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.keyspace.KeyspaceManager;
import grakn.core.server.session.cache.KeyspaceCache;
import grakn.core.server.util.LockManager;
import org.janusgraph.core.JanusGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private final Map<Keyspace, KeyspaceCacheContainer> keyspaceCacheMap;

    public SessionFactory(LockManager lockManager, JanusGraphFactory janusGraphFactory, KeyspaceManager keyspaceManager, Config config) {
        this.janusGraphFactory = janusGraphFactory;
        this.lockManager = lockManager;
        this.keyspaceManager = keyspaceManager;
        this.config = config;
        this.keyspaceCacheMap = new HashMap<>();
    }

    /**
     * Retrieves the {@link Session} needed to open the {@link Transaction}.
     * This will open a new one {@link Session} if it hasn't been opened before
     *
     * @param keyspace The {@link Keyspace} of the {@link Session} to retrieve
     * @return a new {@link Session} connecting to the provided {@link Keyspace}
     */
    public SessionImpl session(KeyspaceImpl keyspace) {
        JanusGraph graph;
        KeyspaceCache cache;
        KeyspaceCacheContainer cacheContainer;

        Lock lock = lockManager.getLock(getLockingKey(keyspace));
        lock.lock();

        try {
            // If keyspace reference already in cache, retrieve open graph and keyspace cache
            if (keyspaceCacheMap.containsKey(keyspace)) {
                graph = keyspaceCacheMap.get(keyspace).graph();
                cache = keyspaceCacheMap.get(keyspace).cache();
                cacheContainer = keyspaceCacheMap.get(keyspace);
            } else { // If keyspace reference not cached, put keyspace in keyspace manager, open new graph and instantiate new keyspace cache
                keyspaceManager.putKeyspace(keyspace);
                graph = janusGraphFactory.openGraph(keyspace.name());
                cache = new KeyspaceCache(config);
                cacheContainer = new KeyspaceCacheContainer(cache, graph);
                keyspaceCacheMap.put(keyspace, cacheContainer);
            }
            SessionImpl session = new SessionImpl(keyspace, config, cache, graph);
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
    public void deleteKeyspace(KeyspaceImpl keyspace) {
        Lock lock = lockManager.getLock(getLockingKey(keyspace));
        lock.lock();
        try {
            if (keyspaceCacheMap.containsKey(keyspace)) {
                KeyspaceCacheContainer container = keyspaceCacheMap.remove(keyspace);
                container.graph().close();
                container.invalidateSessions();
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * Callback function invoked by Session when it gets closed.
     * This access the keyspaceCacheMap to remove the reference of closed session.
     *
     * @param session SessionImpl that is being closed
     */
    private void onSessionClose(SessionImpl session) {
        Lock lock = lockManager.getLock(getLockingKey(session.keyspace()));
        lock.lock();
        try {
            KeyspaceCacheContainer cacheContainer = keyspaceCacheMap.get(session.keyspace());
            cacheContainer.removeSessionReference(session);
            // If there are no more sessions associated to current keyspace,
            // close graph and remove reference from cache.
            if (cacheContainer.referenceCount() == 0) {
                JanusGraph graph = cacheContainer.graph();
                graph.close();
                keyspaceCacheMap.remove(session.keyspace());
            }
        } finally {
            lock.unlock();
        }
    }

    private static String getLockingKey(Keyspace keyspace) {
        return "/keyspace-lock/" + keyspace.name();
    }


    /**
     * Helper class used to hold in memory a reference to a graph together with its schema cache and a reference to all sessions open to the graph.
     */
    private class KeyspaceCacheContainer {

        private KeyspaceCache keyspaceCache;
        // Graph is cached here because concurrently created sessions don't see writes to JanusGraph DB cache
        private JanusGraph graph;
        // Keep track of sessions so that if a user deletes a keyspace we make sure to invalidate all associated sessions
        private List<SessionImpl> sessions;

        KeyspaceCacheContainer(KeyspaceCache keyspaceCache, JanusGraph graph) {
            this.keyspaceCache = keyspaceCache;
            this.graph = graph;
            sessions = new ArrayList<>();
        }

        public KeyspaceCache cache() {
            return keyspaceCache;
        }

        int referenceCount() {
            return sessions.size();
        }

        void addSessionReference(SessionImpl session) {
            sessions.add(session);
        }

        void removeSessionReference(SessionImpl session) {
            sessions.remove(session);
        }

        void invalidateSessions() {
            sessions.forEach(SessionImpl::invalidate);
        }

        public JanusGraph graph() {
            return graph;
        }

    }

}