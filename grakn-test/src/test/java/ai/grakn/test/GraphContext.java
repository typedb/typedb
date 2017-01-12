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

package ai.grakn.test;

import ai.grakn.GraknGraph;
import ai.grakn.engine.backgroundtasks.standalone.StandaloneTaskManager;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.factory.GraphFactory;
import org.junit.rules.ExternalResource;

import java.util.UUID;
import java.util.function.Consumer;

import static ai.grakn.engine.util.ConfigProperties.TASK_MANAGER_INSTANCE;
import static ai.grakn.test.GraknTestEnv.ensureCassandraRunning;
import static ai.grakn.test.GraknTestEnv.ensureHTTPRunning;
import static ai.grakn.test.GraknTestEnv.usingOrientDB;
import static ai.grakn.graphs.TestGraph.loadFromFile;

/**
 *
 * @author alexandraorth
 */
public class GraphContext extends ExternalResource {

    private GraknGraph graph;
    private Consumer<GraknGraph> preLoad;
    private String[] files;

    private GraphContext(){}

    public static GraphContext empty(){
        return new GraphContext();
    }

    public static GraphContext preLoad(Consumer<GraknGraph> build){
        return new GraphContext().setPreLoad(build);
    }

    public static GraphContext preLoad(String... filesToLoad){
        return new GraphContext().setFiles(filesToLoad);
    }

    public GraphContext setPreLoad(Consumer<GraknGraph> preLoad) {
        this.preLoad = preLoad;
        return this;
    }

    public GraphContext setFiles(String[] files) {
        this.files = files;
        return this;
    }

    public GraknGraph graph(){
        return graph;
    }

    @Override
    protected void before() throws Throwable {
        ensureCassandraRunning();

        //TODO remove when Bug #12029 fixed
        ConfigProperties.getInstance().setConfigProperty(TASK_MANAGER_INSTANCE, StandaloneTaskManager.class.getName());
        ensureHTTPRunning();
        //TODO finish remove

        // create the graph
        graph = graphWithNewKeyspace();

        // if data should be pre-loaded, load
        if(preLoad != null){
            preLoad.accept(graph);
        }

        if(files != null){
            for(int i = 0; i < files.length; i++){
                String file = files[i];
                loadFromFile(graph, file);
            }
        }
    }

    @Override
    protected void after() {
        // close the graph
        if(!graph.isClosed()) {
            graph.clear();
            graph.close();
        }
    }

    public static GraknGraph graphWithNewKeyspace() {
        String keyspace;
        if (usingOrientDB()) {
            keyspace = "memory";
        } else {
            // Embedded Casandra has problems dropping keyspaces that start with a number
            keyspace = "a"+ UUID.randomUUID().toString().replaceAll("-", "");
        }

        return GraphFactory.getInstance().getGraph(keyspace);
    }
}
