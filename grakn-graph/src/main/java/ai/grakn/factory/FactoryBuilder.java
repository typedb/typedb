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

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tinkerpop.shaded.minlog.Log;

class FactoryBuilder {
    private static final String FACTORY = "factory.internal";
    private static final Map<String, InternalFactory> openFactories = new ConcurrentHashMap<>();

    private FactoryBuilder(){
        throw new UnsupportedOperationException();
    }

    static InternalFactory getFactory(String keyspace, String engineUrl, String config){
        try{
        	String factoryType = null;
        	if (!Grakn.IN_MEMORY.equals(engineUrl)) 
        		try (FileInputStream fis = new FileInputStream(config)) {
		            ResourceBundle bundle = new PropertyResourceBundle(fis);
		            factoryType = bundle.getString(FACTORY);
		            fis.close();
        		}
        	else
        		factoryType = TinkerInternalFactory.class.getName();
            return getGraknGraphFactory(factoryType, keyspace, engineUrl, config);
        } catch (IOException e) {
            throw new IllegalArgumentException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(config), e);
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
    static InternalFactory getGraknGraphFactory(String factoryType, String keyspace, String engineUrl, String config){
        String key = factoryType + keyspace.toLowerCase();
        Log.debug("Get factory for " + key);
        InternalFactory factory = openFactories.get(key);
        if (factory != null)
        	return factory;
    	synchronized (openFactories) {    		
            InternalFactory internalFactory;
            try {
                //internalFactory = (InternalFactory) Class.forName(factoryType).newInstance();
                internalFactory = (InternalFactory) Class.forName(factoryType)
                        .getDeclaredConstructor(String.class, String.class, String.class)
                        .newInstance(keyspace, engineUrl, config);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw new IllegalArgumentException(ErrorMessage.INVALID_FACTORY.getMessage(factoryType), e);
            }
            openFactories.put(key, internalFactory);
            Log.debug("New factory created " + internalFactory);
            if (keyspace.equalsIgnoreCase(SystemKeyspace.SYSTEM_GRAPH_NAME)) {
            	Log.info("This is a system factory, loading system ontology.");
            	new SystemKeyspace(engineUrl, config).loadSystemOntology();
            }
            else
            	Log.debug("This is not a system factory, not loading system ontology.");
            return internalFactory;
    	}
    }
}
