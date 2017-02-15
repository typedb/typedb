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
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.controller.CommitLogController;
import ai.grakn.factory.EngineGraknGraphFactory;
import org.junit.rules.ExternalResource;
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
public class GraphContext extends ExternalResource {

    private GraknGraph graph;
    private Consumer<GraknGraph> preLoad;
    private String[] files;

    private final static AtomicInteger numberActiveContexts = new AtomicInteger(0);

    private GraphContext(Consumer<GraknGraph> build, String[] files){
        this.preLoad = build;
        this.files = files;
    }

    public static GraphContext empty(){
        return new GraphContext(null, null);
    }

    public static GraphContext preLoad(Consumer<GraknGraph> build){
        return new GraphContext(build, null);
    }

    public static GraphContext preLoad(String... filesToLoad){
        return new GraphContext(null, filesToLoad);
    }

    public GraknGraph graph(){
        return graph;
    }

    public void rollback(){
        try {
            graph.rollback();
        } catch (UnsupportedOperationException e) {
            // If operation unsupported, make a fresh graph
            closeGraph();
            loadGraph();
        }
    }

    @Override
    protected void before() throws Throwable {
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
    protected void after() {
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
        //TODO: get rid of another ugly cast
        graph = (GraknGraph) EngineGraknGraphFactory.getInstance().getGraph(randomKeyspace());

        // if data should be pre-loaded, load
        if(preLoad != null){
            preLoad.accept(graph);
        }

        if(files != null){
            for (String file : files) {
                loadFromFile(graph, file);
            }
        }
    }
}
