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

package ai.grakn.engine;

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSystemProperty;
import ai.grakn.Keyspace;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.test.TxFactoryContext;
import ai.grakn.util.MockRedisRule;
import ai.grakn.util.SimpleURI;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.fail;

public class GraknEngineStartTest {

    private static final int[] PORTS = {50120, 50121, 50122};
    private static final int REDIS_PORT = 50123;

    @ClassRule
    public static MockRedisRule mockRedisRule = MockRedisRule.create(REDIS_PORT);

    @ClassRule
    public static final TxFactoryContext txFactoryContext = TxFactoryContext.create();

    @BeforeClass
    public static void setUpClass() {
        GraknSystemProperty.CURRENT_DIRECTORY.set(StandardSystemProperty.USER_DIR.value());
    }

    @Test
    public void whenStartingMultipleEngines_InitializationSucceeds() throws InterruptedException {
        HashSet<CompletableFuture<Void>> cfs = new HashSet<>();
        for(int port : PORTS) {
            cfs
                    .add(CompletableFuture.supplyAsync(() -> {
                        GraknEngineServer engine = makeEngine(port);
                        engine.start();
                        return engine;
                    })
                    .thenAccept(GraknEngineServer::close)
                    .handle((result, exception) -> handleException(exception)));
        }
        CompletableFuture.allOf(cfs.toArray(new CompletableFuture[cfs.size()])).join();
    }

    @Test
    public void whenStartingAndCreatingKeyspace_InitializationSucceeds() throws InterruptedException {
        HashSet<CompletableFuture<Void>> cfs = new HashSet<>();
        final GraknEngineServer engine = makeEngine(PORTS[0]);
        cfs
                .add(CompletableFuture.runAsync(engine::start));
        cfs
                .add(CompletableFuture.runAsync(() -> {
                    while(!engine.getGraknEngineStatus().isReady()) {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            fail();
                        }
                    }
                    engine.factory().systemKeyspace().openKeyspace(Keyspace.of("grakn"));
                }));
        CompletableFuture.allOf(cfs.toArray(new CompletableFuture[cfs.size()])).join();
        engine.close();
    }

    private Void handleException(Throwable exception) {
        if (exception != null) {
            exception.printStackTrace();
            fail("Could not initialize engine successfully");
        }
        return null;
    }

    private GraknEngineServer makeEngine(int port) {
        GraknEngineConfig graknEngineConfig = GraknEngineConfig.create();
        graknEngineConfig.setConfigProperty(GraknConfigKey.SERVER_PORT, port);
        graknEngineConfig.setConfigProperty(GraknConfigKey.REDIS_HOST, ImmutableList.of(new SimpleURI("localhost", REDIS_PORT).toString()));
        return EngineTestHelper.cleanGraknEngineServer(graknEngineConfig);
    }
}
