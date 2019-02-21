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
    private final ConcurrentHashMap<Keyspace, Integer> keyspaceCacheUseCounts;

    public SessionFactory(LockManager lockManager, Config config, KeyspaceManager keyspaceStore) {
        this.config = config;
        this.lockManager = lockManager;
        this.keyspaceStore = keyspaceStore;
        this.keyspaceCacheMap = new ConcurrentHashMap<>();
        this.keyspaceCacheUseCounts = new ConcurrentHashMap<>();
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
        // TODO this is wasteful after an initialiseNewKeyspace
        return newSessionImpl(keyspace, config);
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
            SessionImpl session = newSessionImpl(keyspace, config);

            // Add current keyspace to list of available Grakn keyspaces
            keyspaceStore.addKeyspace(keyspace);
            session.close();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Obtain a new session to a keyspace, while doing the required accounting
     * for reference counting keyspace cache usage to ensure proper keyspaceCache eviction
     * @param keyspace
     * @param config
     * @return
     */
    private SessionImpl newSessionImpl(Keyspace keyspace, Config config) {

        SessionImpl session;

        // handle two concurrent opening sessions to a new/prexisting keyspace with no active sessions
        Lock lock = lockManager.getLock(getLockingKey(keyspace));
        // WARNING this MUST be a re-entrant lock otherwise we deadlock right here!
        lock.lock();
        try {
            if (!keyspaceCacheMap.containsKey(keyspace)) {
                keyspaceCacheMap.put(keyspace, new KeyspaceCache(config));
                // do cache reference counting for proper eviction
                keyspaceCacheUseCounts.put(keyspace, 0);
            }

            // atomic increment of the count
            keyspaceCacheUseCounts.put(keyspace, keyspaceCacheUseCounts.get(keyspace) + 1);

        } finally {
            lock.unlock();
        }

        // These don't need to be locked:
        // keyspaceCache won't be evicted until the SessionImpl created here is closed
        // it may be written to before by a thread that interrupts right here, but this is fine
        KeyspaceCache keyspaceCache = keyspaceCacheMap.get(keyspace);
        session = new SessionImpl(keyspace, config, keyspaceCache, () -> onSessionClose(keyspace));
        return session;
    }

    private void onSessionClose(Keyspace keyspace) {
        Lock lock = lockManager.getLock(getLockingKey(keyspace));
        lock.lock();
        try {
            // atomically update the keyspaceCache reference count
            Integer keyspaceCacheReferenceCount = keyspaceCacheUseCounts.get(keyspace);
            int newCount = keyspaceCacheReferenceCount - 1;

            if (newCount == 0) {
                // if this is the last session using the keyspace, evict it the cache and corresponding counter
                keyspaceCacheMap.remove(keyspace);
                keyspaceCacheUseCounts.remove(keyspace);
            } else {
                // otherwise, just update the count atomically
                keyspaceCacheUseCounts.put(keyspace, newCount);
            }
        } finally {
            lock.unlock();
        }

        // by the end of this method, the reference count have been atomically updated (or removed)
        // and the keyspaceCache has been removed is necessary
    }

    private static String getLockingKey(Keyspace keyspace) {
        return "/keyspace-lock/" + keyspace.getName();
    }

}