/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.factory;

import ai.grakn.Grakn;
import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.GraknKeyspaceStore;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.factory.FactoryBuilder;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * <p>
 * Engine's internal {@link GraknTx} Factory
 * </p>
 * <p>
 * <p>
 * This internal factory is used to produce {@link GraknTx}s.
 * <p>
 * It is also worth noting that both this class and {@link Grakn#session(String, String)} us the same
 * {@link FactoryBuilder}. This means that graphs produced from either factory pointing to the same keyspace
 * are actually the same graphs.
 * </p>
 *
 * @author fppt
 */
public class EngineGraknTxFactory {
    private final GraknConfig engineConfig;
    private final GraknKeyspaceStore graknKeyspaceStore;
    private final Map<Keyspace, EmbeddedGraknSession> openedSessions;
    private final LockProvider lockProvider;

    public static EngineGraknTxFactory create(LockProvider lockProvider, GraknConfig engineConfig, GraknKeyspaceStore keyspaceStore) {
        return new EngineGraknTxFactory(engineConfig, lockProvider, keyspaceStore);
    }

    private EngineGraknTxFactory(GraknConfig engineConfig, LockProvider lockProvider, GraknKeyspaceStore keyspaceStore) {
        this.openedSessions = new HashMap<>();
        this.engineConfig = engineConfig;
        this.lockProvider = lockProvider;
        this.graknKeyspaceStore = keyspaceStore;
    }

    //Should only be used for testing
    @VisibleForTesting
    public synchronized void refreshConnections() {
        FactoryBuilder.refresh();
    }


    public EmbeddedGraknTx<?> tx(Keyspace keyspace, GraknTxType type) {
        if (!graknKeyspaceStore.containsKeyspace(keyspace)) {
            initialiseNewKeyspace(keyspace);
        }

        return session(keyspace).open(type);
    }

    /**
     * Retrieves the {@link GraknSession} needed to open the {@link GraknTx}.
     * This will open a new one {@link GraknSession} if it hasn't been opened before
     *
     * @param keyspace The {@link Keyspace} of the {@link GraknSession} to retrieve
     * @return a new or existing {@link GraknSession} connecting to the provided {@link Keyspace}
     */
    private EmbeddedGraknSession session(Keyspace keyspace) {
        if (!openedSessions.containsKey(keyspace)) {
            openedSessions.put(keyspace, EmbeddedGraknSession.createEngineSession(keyspace, engineURI(), engineConfig));
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
        Lock lock = lockProvider.getLock(getLockingKey(keyspace));
        lock.lock();
        try {
            // Create new empty keyspace in db
            session(keyspace).open(GraknTxType.WRITE).close();
            // Add current keyspace to list of available Grakn keyspaces
            graknKeyspaceStore.addKeyspace(keyspace);
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

    public GraknKeyspaceStore systemKeyspace() {
        return graknKeyspaceStore;
    }

    private String engineURI() {
        return engineConfig.getProperty(GraknConfigKey.SERVER_HOST_NAME) + ":" + engineConfig.getProperty(GraknConfigKey.SERVER_PORT);
    }
}