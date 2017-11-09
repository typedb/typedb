/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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
import ai.grakn.engine.SystemKeyspace;
import ai.grakn.engine.SystemKeyspaceImpl;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.factory.FactoryBuilder;
import ai.grakn.factory.GraknSessionImpl;
import com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * <p>
 *     Engine's internal {@link GraknTx} Factory
 * </p>
 *
 * <p>
 *     This internal factory is used to produce {@link GraknTx}s.
 *
 *     It is also worth noting that both this class and {@link Grakn#session(String, String)} us the same
 *     {@link FactoryBuilder}. This means that graphs produced from either factory pointing to the same keyspace
 *     are actually the same graphs.
 * </p>
 *
 * @author fppt
 */
public class EngineGraknTxFactory {
    private final Properties properties;
    private final String engineURI;
    private final SystemKeyspace systemKeyspace;
    private final Map<Keyspace, GraknSession> openedSessions;

    @VisibleForTesting //Only used for testing
    public static EngineGraknTxFactory createAndLoadSystemSchema(LockProvider lockProvider, Properties properties) {
        return new EngineGraknTxFactory(properties, lockProvider, true);
    }

    public static EngineGraknTxFactory create(LockProvider lockProvider, Properties properties) {
        return new EngineGraknTxFactory(properties, lockProvider, false);
    }

    private EngineGraknTxFactory(Properties properties, LockProvider lockProvider, boolean loadSchema) {
        this.openedSessions = new HashMap<>();
        this.properties = new Properties();
        this.properties.putAll(properties);
        this.engineURI = properties.getProperty(GraknConfigKey.SERVER_HOST_NAME.name()) + ":" + properties.getProperty(GraknConfigKey.SERVER_PORT.name());
        this.systemKeyspace = SystemKeyspaceImpl.create(this, lockProvider, loadSchema);
    }

    public synchronized void refreshConnections(){
        FactoryBuilder.refresh();
    }

    public GraknTx tx(String keyspace, GraknTxType type){
        return tx(Keyspace.of(keyspace), type);
    }

    public GraknTx tx(Keyspace keyspace, GraknTxType type){
        if(!keyspace.equals(SystemKeyspace.SYSTEM_KB_KEYSPACE)) {
            systemKeyspace.openKeyspace(keyspace);
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
    private GraknSession session(Keyspace keyspace){
        if(!openedSessions.containsKey(keyspace)){
            openedSessions.put(keyspace,GraknSessionImpl.createEngineSession(keyspace, engineURI, properties));
        }
        return openedSessions.get(keyspace);
    }

    /**
     * Initialise a new {@link Keyspace} by opening and closing a transaction on it.
     * @param keyspace the new {@link Keyspace} we want to create
     */
    public void initialiseNewKeyspace(Keyspace keyspace) {
        session(keyspace).open(GraknTxType.WRITE).close();
    }

    public Properties properties() {
        return properties;
    }

    public SystemKeyspace systemKeyspace(){
        return systemKeyspace;
    }
}