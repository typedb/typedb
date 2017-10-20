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
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.GraknConfigKey;
import ai.grakn.engine.SystemKeyspace;
import ai.grakn.factory.FactoryBuilder;

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

    public static EngineGraknTxFactory createAndLoadSystemSchema(Properties properties) {
        return new EngineGraknTxFactory(properties, true);
    }

    public static EngineGraknTxFactory create(Properties properties) {
        return new EngineGraknTxFactory(properties, false);
    }

    private EngineGraknTxFactory(Properties properties, boolean loadSchema) {
        this.properties = new Properties();
        this.properties.putAll(properties);
        this.engineURI = properties.getProperty(GraknConfigKey.SERVER_HOST_NAME.name()) + ":" + properties.getProperty(GraknConfigKey.SERVER_PORT.name());
        this.systemKeyspace = new SystemKeyspace(this, loadSchema);
    }

    public synchronized void refreshConnections(){
        FactoryBuilder.refresh();
    }

    public GraknTx tx(String keyspace, GraknTxType type){
        return tx(Keyspace.of(keyspace), type);
    }

    public GraknTx tx(Keyspace keyspace, GraknTxType type){
        if(!keyspace.equals(SystemKeyspace.SYSTEM_KB_KEYSPACE)) {
            systemKeyspace.ensureKeyspaceInitialised(keyspace);
        }
        return FactoryBuilder.getFactory(keyspace, engineURI, properties).open(type);
    }

    public Properties properties() {
        return properties;
    }

    public SystemKeyspace systemKeyspace(){
        return systemKeyspace;
    }
}