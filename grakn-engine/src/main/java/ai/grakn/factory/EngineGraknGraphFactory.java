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
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.graph.EngineGraknGraph;
import ai.grakn.util.ErrorMessage;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * <p>
 *     Engine's internal Graph Factory
 * </p>
 *
 * <p>
 *     This internal factory is used to produce {@link EngineGraknGraph}s. These are different from {@link GraknGraph}s
 *     in that they provide more low level functionality. Also these graphs do not submit commit logs via the REST API.
 *
 *     It is also worth noting that both this class and {@link Grakn#factory(String, String)} us the same
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
        String pathToConfig = ConfigProperties.getInstance().getPath(ConfigProperties.GRAPH_CONFIG_PROPERTY);

        try(FileInputStream input = new FileInputStream(pathToConfig)){
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(pathToConfig), e);
        }
    }

    public synchronized EngineGraknGraph getGraph(String keyspace) {
        return getGraph(keyspace, false);
    }

    public synchronized EngineGraknGraph getGraphBatchLoading(String keyspace) {
        return getGraph(keyspace, true);
    }

    public synchronized void refreshConnections(){
        FactoryBuilder.refresh();
    }

    private EngineGraknGraph getGraph(String keyspace, boolean batchLoading){
        //TODO: Get rid of ugly casting
        return (EngineGraknGraph) FactoryBuilder.getFactory(keyspace, Grakn.DEFAULT_URI, properties).getGraph(batchLoading);
    }
}


