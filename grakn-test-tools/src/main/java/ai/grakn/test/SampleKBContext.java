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

import ai.grakn.GraknTx;
import ai.grakn.GraknSystemProperty;
import ai.grakn.util.SampleKBLoader;
import org.junit.rules.ExternalResource;

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
 *     This specific class extend {@link SampleKBLoader} and starts Cassandra instance via
 *     {@link ai.grakn.util.EmbeddedCassandra} if needed.
 * </p>
 *
 * @author borislav, fppt
 *
 */
public class SampleKBContext extends ExternalResource {
    private final SampleKBLoader loader;

    private SampleKBContext(@Nullable Consumer<GraknTx> preLoad){
        loader = SampleKBLoader.preLoad(preLoad);
    }

    public static SampleKBContext empty(){
        return getContext(null);
    }

    public static SampleKBContext load(Consumer<GraknTx> build){
        return getContext(build);
    }

    public static SampleKBContext load(String ... files){
        return getContext((graknGraph) -> {
            for (String file : files) {
                loadFromFile(graknGraph, file);
            }
        });
    }

    private static SampleKBContext getContext(@Nullable Consumer<GraknTx> preLoad){
        GraknTestSetup.startCassandraIfNeeded();
        return new SampleKBContext(preLoad);
    }

    public static void loadFromFile(GraknTx graph, String file) {
        SampleKBLoader.loadFromFile(graph, GraknSystemProperty.PROJECT_RELATIVE_DIR.value() + "/grakn-test-tools/src/main/graql/" + file);
    }

    public GraknTx tx() {
        return loader.tx();
    }

    public void rollback() {
        loader.rollback();
    }
}
