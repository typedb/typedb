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

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.factory.FactoryBuilder;
import ai.grakn.factory.InternalFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * <p>
 *     Builds {@link ai.grakn.GraknGraph} bypassing engine.
 * </p>
 *
 * <p>
 *     A helper class which is used to build grakn graphs for testing purposes.
 *     This class bypasses requiring an instance of engine to be running in the background.
 *     Rather it acquires the necessary properties for building a graph directly from system properties.
 *     This does however mean that commit logs are not submitted and no post processing is ran
 * </p>
 *
 * @author fppt
 */
public class GraphLoader {
    private static final String PROJECT_PROPERTY = "grakn.dir";
    private static final String GRAPH_CONFIG_PROPERTY = "grakn.conf";
    private static final AtomicBoolean propertiesLoaded = new AtomicBoolean(false);
    private static Properties graphConfig;

    private final InternalFactory<?> factory;
    private final Consumer<GraknGraph> preLoad;
    private GraknGraph graph;

    private GraphLoader(Consumer<GraknGraph> preLoad){
        factory = FactoryBuilder.getFactory(randomKeyspace(), Grakn.IN_MEMORY, properties());
        this.preLoad = preLoad;

        load();
    }

    public static GraphLoader empty(){
        return new GraphLoader(null);
    }

    public static GraphLoader preLoad(Consumer<GraknGraph> build){
        return new GraphLoader(build);
    }

    public GraknGraph graph(){
        if(graph == null || graph.isClosed()){
            graph = factory.open(GraknTxType.WRITE);
        }

        return graph;
    }

    /**
     * Loads the graph using the specified Preloader
     */
    private void load(){
        try (GraknGraph graph = graph()) {
            if(preLoad != null) preLoad.accept(graph);

            graph.commit();
        }
    }

    /**
     * Using system properties the graph config is directly read from file.
     *
     * @return The properties needed to build a graph.
     */
    //TODO Use this method in GraknEngineConfig (It's a duplicate)
    private static Properties properties(){
        if(propertiesLoaded.compareAndSet(false, true)){
            String configFilePath = System.getProperty(GRAPH_CONFIG_PROPERTY);

            if (!Paths.get(configFilePath).isAbsolute()) {
                configFilePath = getProjectPath() + configFilePath;
            }

            graphConfig = new Properties();
            try (FileInputStream inputStream = new FileInputStream(configFilePath)){
                graphConfig.load(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
                throw GraphOperationException.invalidGraphConfig(configFilePath);
            }
        }

        return graphConfig;
    }

    /**
     * @return The project path. If it is not specified as a JVM parameter it will be set equal to
     * user.dir folder.
     */
    //TODO Use this method in GraknEngineConfig (It's a duplicate)
    private static String getProjectPath() {
        if (System.getProperty(PROJECT_PROPERTY) == null) {
            System.setProperty(PROJECT_PROPERTY, System.getProperty("user.dir"));
        }

        return System.getProperty(PROJECT_PROPERTY) + "/";
    }

    //TODO: Cleanup up. Another duplicate method. Currently in GraknTestEnv
    private static String randomKeyspace(){
        // Embedded Casandra has problems dropping keyspaces that start with a number
        return "a"+ UUID.randomUUID().toString().replaceAll("-", "");
    }
}
