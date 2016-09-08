/*
 *  MindmapsDB - A Distributed Semantic Database
 *  Copyright (C) 2016  Mindmaps Research Ltd
 *
 *  MindmapsDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  MindmapsDB is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.factory;

import io.mindmaps.util.ErrorMessage;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

class MindmapsFactoryBuilder {
    private static final String FACTORY = "factory.internal";
    private static final Map<String, MindmapsGraphFactory> openFactories = new HashMap<>();

    private MindmapsFactoryBuilder(){
        throw new UnsupportedOperationException();
    }

    static MindmapsGraphFactory getFactory(String pathToConfig){
        try {
            FileInputStream fis = new FileInputStream(pathToConfig);
            ResourceBundle bundle = new PropertyResourceBundle(fis);
            fis.close();
            return getFactory(bundle);
        } catch (IOException e) {
            throw new IllegalArgumentException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(pathToConfig), e);
        }
    }

    static MindmapsGraphFactory getFactory(ResourceBundle bundle){
        try{
            return getMindmapsGraphFactory(bundle.getString(FACTORY));
        } catch(MissingResourceException e){
            throw new IllegalArgumentException(ErrorMessage.MISSING_FACTORY_DEFINITION.getMessage());
        }
    }

    /**
     *
     * @param factoryType The string defining which factory should be used for creating the mindmaps graph.
     *                    A valid example includes: io.mindmaps.factory.MindmapsTinkerGraphFactory
     * @return A graph factory which produces the relevant expected graph.
    */
    private static MindmapsGraphFactory getMindmapsGraphFactory(String factoryType){
        if(!openFactories.containsKey(factoryType)) {
            MindmapsGraphFactory mindmapsGraphFactory;
            try {
                mindmapsGraphFactory = (MindmapsGraphFactory) Class.forName(factoryType).newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new IllegalArgumentException(ErrorMessage.INVALID_FACTORY.getMessage(factoryType));
            }
            openFactories.put(factoryType, mindmapsGraphFactory);
        }
        return openFactories.get(factoryType);
    }
}
