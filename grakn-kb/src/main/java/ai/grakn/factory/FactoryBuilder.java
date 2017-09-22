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

import ai.grakn.Keyspace;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.shaded.minlog.Log;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *     Builds a {@link TxFactory}
 * </p>
 *
 * <p>
 *     Builds a {@link TxFactory} which is locked to a specific keyspace and engine URL.
 *     This uses refection in order to dynamically build any vendor specific factory which implements the
 *     {@link TxFactory} API.
 *
 *     The factories in this class are treated as singletons.
 * </p>
 *
 * @author fppt
 */
public class FactoryBuilder {
    static final String KB_MODE = "knowledge-base.mode";
    static final String KB_ANALYTICS = "knowledge-base.analytics";
    private static final Map<String, TxFactory<?>> openFactories = new ConcurrentHashMap<>();

    //This is used to map grakn value properties into the underlaying properties
    private static final Map<String, String> factoryMapper = ImmutableMap.of(
            "in-memory", "ai.grakn.factory.TxFactoryTinker",
            "production", "ai.grakn.factory.TxFactoryJanus",
            "distributed", "ai.grakn.factory.TxFactoryJanusHadoop");

    private FactoryBuilder(){
        throw new UnsupportedOperationException();
    }

    public static TxFactory<?> getFactory(Keyspace keyspace, String engineUrl, Properties properties){
        try{
            String factoryType = factoryMapper.get(properties.get(KB_MODE).toString());
            return getFactory(factoryType, keyspace, engineUrl, properties);
        } catch(MissingResourceException e){
            throw new IllegalArgumentException(ErrorMessage.MISSING_FACTORY_DEFINITION.getMessage());
        }
    }

    /**
     *
     * @param factoryType The string defining which factory should be used for creating the grakn graph.
     *                    A valid example includes: ai.grakn.factory.TxFactoryTinker
     * @return A graph factory which produces the relevant expected graph.
    */
    static TxFactory<?> getFactory(String factoryType, Keyspace keyspace, String engineUrl, Properties properties){
        String key = factoryType + keyspace;
        Log.debug("Get factory for " + key);
        TxFactory<?> factory = openFactories.get(key);
        if (factory != null) {
            return factory;
        }

       return newFactory(key, factoryType, keyspace, engineUrl, properties);
    }

    /**
     *
     * @param key A unique string identifying this factory
     * @param factoryType The type of the factory to initialise. Any factory which implements {@link TxFactory}
     * @param keyspace The keyspace of the graph
     * @param engineUrl The location of the running engine instance
     * @param properties Additional properties to apply to the graph
     * @return A new factory bound to a specific keyspace
     */
    private static synchronized TxFactory<?> newFactory(String key, String factoryType, Keyspace keyspace, String engineUrl, Properties properties){
        TxFactory<?> txFactory;
        try {
            txFactory = (TxFactory<?>) Class.forName(factoryType)
                    .getDeclaredConstructor(Keyspace.class, String.class, Properties.class)
                    .newInstance(keyspace, engineUrl, properties);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalArgumentException(ErrorMessage.INVALID_FACTORY.getMessage(factoryType), e);
        }
        openFactories.put(key, txFactory);
        Log.debug("New factory created " + txFactory);
        return txFactory;
    }

    /**
     * Clears all connections.
     */
    //TODO Should this close each of the factories (and wait for all open transactions to be closed?)
    //TODO Calling this from within the code causes a memory leak
    public static void refresh(){
        openFactories.clear();
    }
}
