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
import ai.grakn.util.ErrorMessage;
import org.apache.tinkerpop.shaded.minlog.Log;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *     Builds a Grakn Graph {@link InternalFactory}
 * </p>
 *
 * <p>
 *     Builds a Grakn Graph Factory which is locked to a specific keyspace and engine URL.
 *     This uses refection in order to dynamically build any vendor specific factory which implements the
 *     {@link InternalFactory} API.
 *
 *     The factories in this class are treated as singletons.
 * </p>
 *
 * @author fppt
 */
public class FactoryBuilder {
    private static final String FACTORY = "factory.internal";
    private static final Map<String, InternalFactory> openFactories = new ConcurrentHashMap<>();

    private FactoryBuilder(){
        throw new UnsupportedOperationException();
    }

    public static InternalFactory getFactory(String keyspace, String engineUrl, Properties properties, SystemKeyspace systemKeyspace){
        try{
            String factoryType;
            if (!Grakn.IN_MEMORY.equals(engineUrl)) {
                factoryType = properties.get(FACTORY).toString();
            } else {
                factoryType = TinkerInternalFactory.class.getName();
            }
            return getGraknGraphFactory(factoryType, keyspace, engineUrl, properties, systemKeyspace);
        } catch(MissingResourceException e){
            throw new IllegalArgumentException(ErrorMessage.MISSING_FACTORY_DEFINITION.getMessage());
        }
    }

    /**
     *
     * @param factoryType The string defining which factory should be used for creating the grakn graph.
     *                    A valid example includes: ai.grakn.factory.TinkerInternalFactory
     * @return A graph factory which produces the relevant expected graph.
    */
    static InternalFactory getGraknGraphFactory(String factoryType, String keyspace, String engineUrl, Properties properties, SystemKeyspace systemKeyspace){
        String key = factoryType + keyspace.toLowerCase();
        Log.debug("Get factory for " + key);
        InternalFactory factory = openFactories.get(key);
        if (factory != null) {
            return factory;
        }

       return newFactory(key, factoryType, keyspace, engineUrl, properties, systemKeyspace);
    }

    /**
     *
     * @param key A unique string identifying this factory
     * @param factoryType The type of the factory to initialise. Any factory which implements {@link InternalFactory}
     * @param keyspace The keyspace of the graph
     * @param engineUrl The location of the running engine instance
     * @param properties Additional properties to apply to the graph
     * @return A new factory bound to a specific keyspace
     */
    private static synchronized InternalFactory newFactory(String key, String factoryType, String keyspace, String engineUrl, Properties properties, SystemKeyspace systemKeyspace){
        InternalFactory<?> internalFactory;
        try {
            internalFactory = (InternalFactory) Class.forName(factoryType)
                    .getDeclaredConstructor(String.class, String.class, Properties.class, SystemKeyspace.class)
                    .newInstance(keyspace, engineUrl, properties, systemKeyspace);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalArgumentException(ErrorMessage.INVALID_FACTORY.getMessage(factoryType), e);
        }
        openFactories.put(key, internalFactory);
        Log.debug("New factory created " + internalFactory);
        return internalFactory;
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
