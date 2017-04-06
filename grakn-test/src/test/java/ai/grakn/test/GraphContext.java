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
import ai.grakn.GraknTxType;
import ai.grakn.engine.cache.EngineCacheStandAlone;
import ai.grakn.factory.EngineGraknGraphFactory;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.function.Consumer;

import static ai.grakn.graphs.TestGraph.loadFromFile;
import static ai.grakn.test.GraknTestEnv.ensureCassandraRunning;
import static ai.grakn.test.GraknTestEnv.randomKeyspace;
import static ai.grakn.test.GraknTestEnv.usingTinker;

/**
 *
 * @author alexandraorth
 */
public class GraphContext implements TestRule {

    private GraknGraph graph;
    private String keyspace;
    private Consumer<GraknGraph> preLoad;
    private String[] files;

    private GraphContext(Consumer<GraknGraph> build, String[] files){
        this.preLoad = build;
        this.files = files;
        this.keyspace = randomKeyspace();
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
        if(graph.isClosed()){
            graph = getEngineGraph();
        }
        return graph;
    }

    public void rollback() {
        if (usingTinker()) {
            graph.admin().clear(EngineCacheStandAlone.getCache());
            loadGraph();
        } else if (!graph.isClosed()) {
            graph.close();
        }
        graph = getEngineGraph();
    }

    public void load(Consumer<GraknGraph> build){
        this.preLoad = build;
        loadGraph();
    }

    private GraknGraph getEngineGraph(){
        return EngineGraknGraphFactory.getInstance().getGraph(keyspace, GraknTxType.WRITE);
    }

    private void loadGraph() {
        try(GraknGraph graph = getEngineGraph()) {

            // if data should be pre-loaded, load
            if (preLoad != null) {
                preLoad.accept(graph);
            }

            if (files != null) {
                for (String file : files) {
                    loadFromFile(graph, file);
                }
            }

            graph.admin().commitNoLogs();
        }
    }

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                ensureCassandraRunning();

                loadGraph();

                try (GraknGraph graph = getEngineGraph()){
                    GraphContext.this.graph = graph;
                    base.evaluate();
                }
            }
        };
    }

}
