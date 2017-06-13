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

package ai.grakn.util;

import ai.grakn.exception.GraphOperationException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>
 *     Builds a Test Grakn Graph
 * </p>
 *
 * <p>
 *     Helper class which aids in creating test and toy graphs.
 *     This helper bypasses the need to have a running engine.
 * </p>
 *
 * @author fppt
 */
public class GraphBuilder {
    private static final String SYSTEM_PROPERTY_GRAKN_CONFIGURATION_FILE = "grakn.conf";
    private static AtomicBoolean propertiesSet = new AtomicBoolean(false);
    private static Properties properties;

    /**
     * Gets the properties needed in order to build a grakn graph.
     *
     * @return Properties needed to build a grakn graph.
     */
    static Properties getSystemProperties(){
        if(propertiesSet.compareAndSet(false, true)){
            String config = System.getProperty(SYSTEM_PROPERTY_GRAKN_CONFIGURATION_FILE);
            if(config == null) throw GraphOperationException.invalidConfig(null);

            properties = new Properties();
            try (FileInputStream inputStream = new FileInputStream(config)){
                properties.load(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return properties;
    }
}
