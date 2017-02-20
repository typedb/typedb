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

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.controller.CommitLogController;
import spark.Spark;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static ai.grakn.graphs.TestGraph.loadFromFile;
import static ai.grakn.test.GraknTestEnv.ensureCassandraRunning;
import static ai.grakn.test.GraknTestEnv.hideLogs;
import static ai.grakn.test.GraknTestEnv.randomKeyspace;

/**
 *
 * @author alexandraorth
 */
public class GraphContext extends EngineContext {
    private String keyspace;
    private GraknGraphFactory factory;
    private GraknGraph graph;
    private Consumer<GraknGraphFactory> preLoad;
    private String[] files;

    private final static AtomicInteger numberActiveContexts = new AtomicInteger(0);

    private GraphContext(Consumer<GraknGraphFactory> build, String[] files){
        super(false, false, true); //This starts in memory engine
        this.preLoad = build;
        this.files = files;
        keyspace = randomKeyspace();
    }

    public static GraphContext empty(){
        return new GraphContext(null, null);
    }

    public static GraphContext preLoad(Consumer<GraknGraphFactory> build){
        return new GraphContext(build, null);
    }

    public static GraphContext preLoad(String... filesToLoad){
        return new GraphContext(null, filesToLoad);
    }

    public GraknGraph graph(){
        if(graph.isClosed()){
            graph = factory().getGraph();
        }
        return graph;
    }

    public GraknGraphFactory factory(){
        if(factory == null){
            factory = Grakn.factory(Grakn.DEFAULT_URI, keyspace);
        }
        return factory;
    }

    @Override
    public void before() throws Throwable {
        super.before();
        hideLogs();

        ensureCassandraRunning();

        //TODO remove when Bug #12029 fixed
        if (numberActiveContexts.getAndIncrement() == 0) {
            new CommitLogController();
            Spark.awaitInitialization();
        }
        //TODO finish remove

        // create the graph
        loadGraph();
    }

    @Override
    public void after() {
        super.after();

        closeGraph();
        if (numberActiveContexts.decrementAndGet() == 0) {
            GraknEngineServer.stopHTTP();
        }
    }

    private void closeGraph(){
        // close the graph
        if(!graph.isClosed()) {
            graph.clear();
            graph.close();
        }
    }

    private void loadGraph() {
        graph = factory().getGraph();

        // if data should be pre-loaded, load
        if(preLoad != null){
            preLoad.accept(factory());
        }

        if(files != null){
            for (String file : files) {
                loadFromFile(factory, file);
            }
        }
    }

    public void clearGraph(){
        graph.clear();
        loadGraph();
    }
}
