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
import ai.grakn.util.GraphLoader;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import javax.annotation.Nullable;
import java.util.function.Consumer;

/**
 * <p>
 *     Sets up graphs for testing
 * </p>
 *
 * <p>
 *     Contains utility methods and statically initialized environment variables to control
 *     Grakn unit tests.
 *
 *     This specific class extend {@link GraphLoader} and starts Cassandra instance via
 *     {@link ai.grakn.util.EmbeddedCassandra} if needed.
 * </p>
 *
 * @author borislav, fppt
 *
 */
public class GraphContext extends GraphLoader implements TestRule {
    private boolean assumption = true;

    private GraphContext(@Nullable Consumer<GraknGraph> preLoad){
        super(preLoad);
    }

    public static GraphContext empty(){
        return getContext(null);
    }

    public static GraphContext preLoad(Consumer<GraknGraph> build){
        return getContext(build);
    }

    public static GraphContext preLoad(String ... files){
        return getContext((graknGraph) -> {
            for (String file : files) {
                loadFromFile(graknGraph, file);
            }
        });
    }

    private static GraphContext getContext(@Nullable Consumer<GraknGraph> preLoad){
        GraknTestSetup.startCassandraIfNeeded();
        return new GraphContext(preLoad);
    }

    public GraphContext assumeTrue(boolean bool){
        this.assumption = bool;
        return this;
    }

    public static void loadFromFile(GraknGraph graph, String file) {
        GraphLoader.loadFromFile(graph, "../../grakn-test-tools/src/main/graql/" + file);
    }

    @Override
    public Statement apply(final Statement base, Description description) {

        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                org.junit.Assume.assumeTrue(assumption);
                base.evaluate();
            }
        };
    }
}
