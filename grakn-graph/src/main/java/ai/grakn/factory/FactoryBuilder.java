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
class FactoryBuilder {
    private static final String FACTORY = "factory.internal";
    private static final Map<String, InternalFactory> openFactories = new ConcurrentHashMap<>();

    private FactoryBuilder(){
        throw new UnsupportedOperationException();
    }

    static InternalFactory getFactory(String keyspace, String engineUrl, Properties properties){
        try{
            String factoryType;
            if (!Grakn.IN_MEMORY.equals(engineUrl)) {
                factoryType = properties.get(FACTORY).toString();
            } else {
                factoryType = TinkerInternalFactory.class.getName();
            }
            return getGraknGraphFactory(factoryType, keyspace, engineUrl, properties);
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
    static InternalFactory getGraknGraphFactory(String factoryType, String keyspace, String engineUrl, Properties properties){
        String key = factoryType + keyspace.toLowerCase();
        Log.debug("Get factory for " + key);
        InternalFactory factory = openFactories.get(key);
        if (factory != null) {
            return factory;
        }

       return newFactory(key, factoryType, keyspace, engineUrl, properties);
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
    private static synchronized InternalFactory newFactory(String key, String factoryType, String keyspace, String engineUrl, Properties properties){
        InternalFactory<?> internalFactory;
        try {
            internalFactory = (InternalFactory) Class.forName(factoryType)
                    .getDeclaredConstructor(String.class, String.class, Properties.class)
                    .newInstance(keyspace, engineUrl, properties);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalArgumentException(ErrorMessage.INVALID_FACTORY.getMessage(factoryType), e);
        }
        openFactories.put(key, internalFactory);
        Log.debug("New factory created " + internalFactory);
        if (keyspace.equalsIgnoreCase(SystemKeyspace.SYSTEM_GRAPH_NAME)) {
            Log.debug("This is a system factory, loading system ontology.");
            new SystemKeyspace<>(internalFactory).loadSystemOntology();
        } else {
            Log.debug("This is not a system factory, not loading system ontology.");
        }
        return internalFactory;
    }

    /**
     * Clears all connections.
     */
    static void refresh(){
        openFactories.clear();
    }
}
