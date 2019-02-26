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
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.keyspace.KeyspaceManager;
import grakn.core.server.session.cache.KeyspaceCache;
import grakn.core.server.util.LockManager;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

/**
 * Grakn Server's internal SessionImpl Factory
 * All components should use this factory so that every time a session to a new keyspace gets created
 * it is possible to also update the Keyspace Store (which tracks all existing keyspaces).
 */
public class SessionFactory {
    private final JanusGraphFactory janusGraphFactory;
    private final KeyspaceManager keyspaceStore;
    private Config config;
    private final LockManager lockManager;

    private final ConcurrentHashMap<KeyspaceImpl, KeyspaceCacheContainer> keyspaceCacheMap;

    public SessionFactory(LockManager lockManager, JanusGraphFactory janusGraphFactory, KeyspaceManager keyspaceStore, Config config) {
        this.janusGraphFactory = janusGraphFactory;
        this.lockManager = lockManager;
        this.keyspaceStore = keyspaceStore;
        this.config = config;
        this.keyspaceCacheMap = new ConcurrentHashMap<>();
    }

    /**
     * Retrieves the Session needed to open the Transaction.
     * This will open a new one Session if it hasn't been opened before
     *
     * @param keyspace The Keyspace of the Session to retrieve
     * @return a new or existing Session connecting to the provided Keyspace
     */
    public SessionImpl session(KeyspaceImpl keyspace) {
        if (!keyspaceStore.containsKeyspace(keyspace)) {
            return initialiseNewKeyspace(keyspace);
        } else {
            return newSessionImpl(keyspace);
        }
    }

    public synchronized void deleteKeyspace(KeyspaceImpl keyspace) {

        keyspaceCacheMap.compute(keyspace, (ksp, keyspaceCacheContainer) -> {
            // if a concurrent delete has not occurred already
            if (keyspaceCacheContainer != null) {
                // atomically close the shared graph, blocking concurrent users from re-creating it
                JanusGraph graph = keyspaceCacheContainer.retrieveGraph();
                graph.close();
            }
            // null out the value so others can re-create keyspace
            return null;
        });

        // don't delete the entry, in case it has already been recreated meanwhile, see comments above
    }

    /**
     * Initialise a new Keyspace by opening and closing a transaction on it.
     *
     * @param keyspace the new Keyspace we want to create
     */
    private SessionImpl initialiseNewKeyspace(KeyspaceImpl keyspace) {
        //If the keyspace does not exist lock and create it
        Lock lock = lockManager.getLock(getLockingKey(keyspace));
        lock.lock();
        SessionImpl session;
        try {
            // opening a session initialises the keyspace with meta concepts, protect with lock so doesn't get initialised twice
            session = newSessionImpl(keyspace);
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
     * @return
     */
    private SessionImpl newSessionImpl(KeyspaceImpl keyspace) {


        SessionImpl session;
        keyspaceCacheMap.compute(keyspace, (ksp, keyspaceCacheContainer) -> {
            // atomically upsert a new KeyspaceCacheContainer
            // AND increment the reference count atomically
            KeyspaceCacheContainer cacheContainer;
            if (keyspaceCacheContainer == null) {
                JanusGraph graph = janusGraphFactory.openGraph(keyspace.name());
                cacheContainer = new KeyspaceCacheContainer(new KeyspaceCache(config), graph);
            } else {
                cacheContainer = keyspaceCacheContainer;
            }
            cacheContainer.incrementReferenceCount();
            return cacheContainer;
        });

        KeyspaceCacheContainer keyspaceCacheContainer = keyspaceCacheMap.get(keyspace);
        session = new SessionImpl(keyspace, config, keyspaceCacheContainer.retrieveCache(), keyspaceCacheContainer.retrieveGraph(), () -> onSessionClose(keyspace, keyspaceCacheContainer));
        return session;
    }

    private void onSessionClose(KeyspaceImpl keyspace, KeyspaceCacheContainer cacheContainer) {
        // require a reference to the ORIGINAL container, since the key it is mapped to
        // may have been re-mapped to a new instance of KeyspaceCacheContainer
        // eg. if someone subsequently deletes and re-creates the same keyspace

        // when clearing an entry from the map we face the same issues as when deleting a session
        // this would be much neater if sessions could not be deleted at arbitrary times!

        keyspaceCacheMap.compute(keyspace, (ksp, mappedContainer) -> {

            // only update accounting and possibly clear from map IF
            // the container currently in the map is the same we initialised this session with

            if (mappedContainer == cacheContainer) {
                mappedContainer.decrementReferenceCount();
                if (mappedContainer.referenceCount() == 0) {
                    JanusGraph graph = mappedContainer.retrieveGraph();
                    ((StandardJanusGraph) graph).getOpenTransactions().forEach(org.janusgraph.core.Transaction::close);
                    graph.close();
                    // in this case only, we clear the entry in the map
                    return null;
                }
            } else {
                // this means the keyspace has been cleared & recreated, through deletion
                JanusGraph graph = cacheContainer.retrieveGraph();
                // this graph must already have been closed
                assert graph.isClosed();
            }
            return mappedContainer;
        });
    }

    private static String getLockingKey(KeyspaceImpl keyspace) {
        return "/keyspace-lock/" + keyspace.name();
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