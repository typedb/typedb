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

import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.keyspace.Keyspace;
import grakn.core.server.keyspace.KeyspaceManager;
import grakn.core.util.GraknConfig;
import grakn.core.server.util.LockManager;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * Engine's internal {@link Transaction} Factory
 * This internal factory is used to produce {@link Transaction}s.
 */
public class SessionStore {
    private final GraknConfig engineConfig;
    private final KeyspaceManager keyspaceStore;
    private final Map<Keyspace, SessionImpl> openedSessions;
    private final LockManager lockManager;

    public static SessionStore create(LockManager lockManager, GraknConfig engineConfig, KeyspaceManager keyspaceStore) {
        return new SessionStore(engineConfig, lockManager, keyspaceStore);
    }

    private SessionStore(GraknConfig engineConfig, LockManager lockManager, KeyspaceManager keyspaceStore) {
        this.openedSessions = new HashMap<>();
        this.engineConfig = engineConfig;
        this.lockManager = lockManager;
        this.keyspaceStore = keyspaceStore;
    }


    public TransactionImpl<?> tx(Keyspace keyspace, Transaction.Type type) {
        if (!keyspaceStore.containsKeyspace(keyspace)) {
            initialiseNewKeyspace(keyspace);
        }

        return session(keyspace).transaction(type);
    }

    public void closeSessions(){
        this.openedSessions.values().forEach(SessionImpl::close);
    }

    /**
     * Retrieves the {@link Session} needed to open the {@link Transaction}.
     * This will open a new one {@link Session} if it hasn't been opened before
     *
     * @param keyspace The {@link Keyspace} of the {@link Session} to retrieve
     * @return a new or existing {@link Session} connecting to the provided {@link Keyspace}
     */
    private SessionImpl session(Keyspace keyspace){
        if(!openedSessions.containsKey(keyspace)){
            openedSessions.put(keyspace, SessionImpl.createEngineSession(keyspace, engineConfig, TransactionFactoryBuilder.getInstance()));
        }
        return openedSessions.get(keyspace);
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
            // Create new empty keyspace in db
            session(keyspace).transaction(Transaction.Type.WRITE).close();
            // Add current keyspace to list of available Grakn keyspaces
            keyspaceStore.addKeyspace(keyspace);
        } finally {
            lock.unlock();
        }
    }

    private static String getLockingKey(Keyspace keyspace) {
        return "/creating-new-keyspace-lock/" + keyspace.getValue();
    }

    public GraknConfig config() {
        return engineConfig;
    }

    public KeyspaceManager keyspaceStore() {
        return keyspaceStore;
    }

}