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

    private final ConcurrentHashMap<Keyspace, KeyspaceCache> keyspaceCacheMap;

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
            initialiseNewKeyspace(keyspace);
        }
        return new SessionImpl(keyspace, config, keyspaceCacheMap.get(keyspace));
    }

    /**
     * Initialise a new {@link Keyspace} by opening and closing a transaction on it.
     *
     * @param keyspace the new {@link Keyspace} we want to create
     */
    private void initialiseNewKeyspace(Keyspace keyspace) {
        //If the keyspace does not exist lock and create it
        Lock lock = lockManager.getLock(getLockingKey(keyspace));
        lock.lock();
        try {

            // create new KeyspaceCache
            KeyspaceCache keyspaceCache = new KeyspaceCache(config);
            keyspaceCacheMap.put(keyspace, keyspaceCache);

            // Create new empty keyspace in db
            SessionImpl session = new SessionImpl(keyspace, config, keyspaceCache);

            // Add current keyspace to list of available Grakn keyspaces
            keyspaceStore.addKeyspace(keyspace);
            session.close();
        } finally {
            lock.unlock();
        }
    }


    private static String getLockingKey(Keyspace keyspace) {
        return "/creating-new-keyspace-lock/" + keyspace.getName();
    }

}