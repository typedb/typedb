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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

/**
 * Grakn Server's internal {@link SessionImpl} Factory
 * All components should use this factory so that every time a session to a new keyspace gets created
 * it is possible to also update the Keyspace Store (which tracks all existing keyspaces).
 */
public class SessionFactory {
    private final Config config;
    private final KeyspaceManager keyspaceStore;
    private final LockManager lockManager;

    private final ConcurrentHashMap<Keyspace, KeyspaceCacheContainer> keyspaceCacheMap;

    public SessionFactory(LockManager lockManager, Config config, KeyspaceManager keyspaceStore) {
        this.config = config;
        this.lockManager = lockManager;
        this.keyspaceStore = keyspaceStore;
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
        if (!keyspaceStore.containsKeyspace(keyspace)) {
            return initialiseNewKeyspace(keyspace);
            // TODO could return new session directly here so we don't close and re-open right away
        } else {
            return newSessionImpl(keyspace, config);
        }
    }

    public void deleteKeyspace(Keyspace keyspace) {

        // problem on how to implement this in a thread-safe way:
        // two tasks: close JanusGraph, and remove entry from the keyspace cache map

        // option 1: atomically [close JanusGraph, null value in map], [delete null entry in map]
        // option 2: atomically [remove valid entry from map], [close JanusGraph]

        // problem option 1: map may have a new, valid entry inserted right after we null the value atomically but before we delete it
        // so we end up deleting a valid entry

        // problem option 2: remove entry from map, second thread creates a new entry and tries to open a new JanusGraph with
        // the same name before we are able to delete the close/delete the old JanusGraph


        // just null out value for now, allow nulls to accumulate
        keyspaceCacheMap.compute(keyspace, (ksp, keyspaceCacheContainer) -> {
            // if a concurrent delete has not occurred already
            if (keyspaceCacheContainer != null) {
                // atomically clear the graph, blocking concurrent users from creating
                JanusGraph graph = keyspaceCacheContainer.retrieveGraph();
                graph.close();
                JanusGraphFactory.drop(graph);
            }
            // null out the value so others can re-create keyspace
            return null;
        });

        // don't delete the entry, in case it has already been recreated meanwhile, see comments above
    }

    /**
     * Initialise a new {@link Keyspace} by opening and closing a transaction on it.
     *
     * @param keyspace the new {@link Keyspace} we want to create
     */
    private SessionImpl initialiseNewKeyspace(Keyspace keyspace) {
        //If the keyspace does not exist lock and create it
        Lock lock = lockManager.getLock(getLockingKey(keyspace));
        lock.lock();
        SessionImpl session;
        try {
            // opening a session initialises the keyspace with meta concepts, protect with lock so doesn't get initialised twice
            session = newSessionImpl(keyspace, config);
            // Add current keyspace to list of available Grakn keyspaces
            keyspaceStore.addKeyspace(keyspace);
        } finally {
            lock.unlock();
        }
        return session;
    }

    /**
     * Obtain a new session to a keyspace, while doing the required accounting
     * for reference counting keyspace cache usage to ensure proper keyspaceCache eviction
     *
     * @param keyspace
     * @param config
     * @return
     */
    private SessionImpl newSessionImpl(Keyspace keyspace, Config config) {

        SessionImpl session;
        keyspaceCacheMap.compute(keyspace, (ksp, keyspaceCacheContainer) -> {
            // atomically upsert a new KeyspaceCacheContainer
            // AND increment the reference count atomically
            KeyspaceCacheContainer cacheContainer;
            if (keyspaceCacheContainer == null) {
                JanusGraph graph = JanusGraphFactory.openGraph(keyspace.getName(), config);
                cacheContainer = new KeyspaceCacheContainer(new KeyspaceCache(config), graph);
            } else {
                cacheContainer = keyspaceCacheContainer;
            }
            cacheContainer.incrementReferenceCount();
            return cacheContainer;
        });

        KeyspaceCacheContainer keyspaceCacheContainer = keyspaceCacheMap.get(keyspace);
        session = new SessionImpl(keyspace, config, keyspaceCacheContainer.retrieveCache(), keyspaceCacheContainer.retrieveGraph(), () -> onSessionClose(keyspace));
        return session;
    }

    private void onSessionClose(Keyspace keyspace) {
        KeyspaceCacheContainer cacheContainer = keyspaceCacheMap.get(keyspace);
        cacheContainer.decrementReferenceCount();
        if (cacheContainer.referenceCount() == 0) {
            JanusGraph graph = cacheContainer.retrieveGraph();
            ((StandardJanusGraph) graph).getOpenTransactions().forEach(org.janusgraph.core.Transaction::close);
            graph.close();
            keyspaceCacheMap.remove(keyspace); // remove container from map
        }
    }

    private static String getLockingKey(Keyspace keyspace) {
        return "/keyspace-lock/" + keyspace.getName();
    }


    private class KeyspaceCacheContainer {

        private KeyspaceCache keyspaceCache;
        private JanusGraph graph; // cached here because concurrently created sessions don't see writes to JanusGraph DB cache
        private int references;

        public KeyspaceCacheContainer(KeyspaceCache keyspaceCache, JanusGraph graph) {
            this.keyspaceCache = keyspaceCache;
            this.graph = graph;
            references = 0;
        }

        public KeyspaceCache retrieveCache() {
            return keyspaceCache;
        }

        public synchronized void incrementReferenceCount() {
            references++;
        }

        public synchronized void decrementReferenceCount() {
            references--;
        }

        public int referenceCount() {
            return references;
        }

        public JanusGraph retrieveGraph() {
            return graph;
        }


    }

}