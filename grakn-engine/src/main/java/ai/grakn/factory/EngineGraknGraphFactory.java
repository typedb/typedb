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

package ai.grakn.factory;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.engine.GraknEngineConfig;

import java.util.Properties;

/**
 * <p>
 *     Engine's internal Graph Factory
 * </p>
 *
 * <p>
 *     This internal factory is used to produce {@link GraknGraph}s.
 *
 *     It is also worth noting that both this class and {@link Grakn#session(String, String)} us the same
 *     {@link FactoryBuilder}. This means that graphs produced from either factory pointing to the same keyspace
 *     are actually the same graphs.
 * </p>
 *
 * @author fppt
 */
public class EngineGraknGraphFactory {
    private final Properties properties;
    private static EngineGraknGraphFactory instance = null;


    public static synchronized EngineGraknGraphFactory getInstance() {
        if (instance == null) {
            instance = new EngineGraknGraphFactory();
        }
        return instance;
    }

    private EngineGraknGraphFactory() {
        properties = new Properties();
        properties.putAll(GraknEngineConfig.getInstance().getProperties());
    }

    public synchronized void refreshConnections(){
        FactoryBuilder.refresh();
    }

    public GraknGraph getGraph(String keyspace, GraknTxType type){
        return FactoryBuilder.getFactory(keyspace, Grakn.DEFAULT_URI, properties).open(type);
    }
}


