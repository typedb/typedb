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

import ai.grakn.util.ErrorMessage;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

class FactoryBuilder {
    private static final String FACTORY = "factory.internal";
    private static final Map<String, InternalFactory> openFactories = new HashMap<>();

    private FactoryBuilder(){
        throw new UnsupportedOperationException();
    }

    static InternalFactory getFactory(String keyspace, String engineUrl, String config){
        try{
            FileInputStream fis = new FileInputStream(config);
            ResourceBundle bundle = new PropertyResourceBundle(fis);
            fis.close();

            return getGraknGraphFactory(bundle.getString(FACTORY), keyspace, engineUrl, config);
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
    private static InternalFactory getGraknGraphFactory(String factoryType, String keyspace, String engineUrl, String config){
        String key = factoryType + keyspace;
        if(!openFactories.containsKey(key)) {
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
        }
        return openFactories.get(key);
    }
}
