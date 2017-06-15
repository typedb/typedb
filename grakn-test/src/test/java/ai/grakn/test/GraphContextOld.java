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
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.function.Consumer;

import static ai.grakn.test.graphs.TestGraph.loadFromFile;
import static ai.grakn.test.GraknTestEngineSetup.randomKeyspace;

/**
 *
 * @author alexandraorth
 */
@Deprecated
public class GraphContextOld implements TestRule {

    private EngineGraknGraphFactory factory;
    private GraknGraph graph;
    private String keyspace;
    private Consumer<GraknGraph> preLoad;
    private String[] files;
    private boolean assumption = true;

    private GraphContextOld(Consumer<GraknGraph> build, String[] files){
        this.factory = EngineGraknGraphFactory.create(GraknEngineConfig.create().getProperties());
        this.preLoad = build;
        this.files = files;
        this.keyspace = randomKeyspace();
    }

    public static GraphContextOld empty(){
        return new GraphContextOld(null, null);
    }

    public static GraphContextOld preLoad(Consumer<GraknGraph> build){
        return new GraphContextOld(build, null);
    }

    public static GraphContextOld preLoad(String... filesToLoad){
        return new GraphContextOld(null, filesToLoad);
    }

    public GraphContextOld assumeTrue(boolean bool){
        this.assumption = bool;
        return this;
    }

    public GraknGraph graph(){
        if(graph.isClosed()){
            graph = getEngineGraph();
        }
        return graph;
    }

    public void load(Consumer<GraknGraph> build){
        this.preLoad = build;
        loadGraph();
    }

    public EngineGraknGraphFactory factory(){
        return factory;
    }

    private GraknGraph getEngineGraph(){
        return factory().getGraph(keyspace, GraknTxType.WRITE);
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
                org.junit.Assume.assumeTrue(assumption);
                GraknTestSetup.startCassandraIfNeeded();

                loadGraph();

                try (GraknGraph graph = getEngineGraph()){
                    GraphContextOld.this.graph = graph;
                    base.evaluate();
                }
            }
        };
    }

}
