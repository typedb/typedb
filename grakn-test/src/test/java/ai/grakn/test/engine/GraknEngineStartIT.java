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
 *
 */

package ai.grakn.test.engine;

import ai.grakn.GraknSystemProperty;
import ai.grakn.engine.GraknEngineConfig;
import static ai.grakn.engine.GraknEngineConfig.REDIS_HOST;
import static ai.grakn.engine.GraknEngineConfig.SERVER_PORT_NUMBER;
import static ai.grakn.engine.GraknEngineConfig.TASK_MANAGER_IMPLEMENTATION;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.tasks.manager.redisqueue.RedisTaskManager;
import ai.grakn.engine.util.SimpleURI;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.util.EmbeddedRedis;
import com.google.common.base.StandardSystemProperty;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.junit.AfterClass;
import static org.junit.Assert.fail;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitQuickcheck.class)
public class GraknEngineStartIT {

    private static final int[] PORTS = {50120, 50121, 50122};
    public static final int REDIS_PORT = 50123;

    @BeforeClass
    public static void setUpClass() {
        EmbeddedRedis.start(REDIS_PORT);
        GraknTestSetup.startCassandraIfNeeded();
        GraknSystemProperty.CURRENT_DIRECTORY.set(StandardSystemProperty.USER_DIR.value());
    }

    @AfterClass
    public static void tearDownClass() {
        EmbeddedRedis.stop();
    }

    @Test
    public void whenStartingMultipleEngines_InitializationSucceeds() throws InterruptedException {
        HashSet<CompletableFuture<Void>> cfs = new HashSet<>();
        for(int port : PORTS) {
            cfs
                    .add(CompletableFuture.supplyAsync(() -> {
                        GraknEngineConfig graknEngineConfig = GraknEngineConfig.create();
                        Properties properties = graknEngineConfig.getProperties();
                        properties.setProperty(SERVER_PORT_NUMBER, String.valueOf(port));
                        properties.setProperty(REDIS_HOST, new SimpleURI("localhost", REDIS_PORT).toString());
                        properties.setProperty(TASK_MANAGER_IMPLEMENTATION, RedisTaskManager.class.getName());
                        GraknEngineServer engine = new GraknEngineServer(graknEngineConfig);
                        engine.start();
                        return engine;
                    })
                    .thenAccept(GraknEngineServer::close)
                    .handle((result, exception) -> {
                        if (exception != null) {
                            exception.printStackTrace();
                            fail("Could not initialize engine successfully");
                        }
                        return null;
                    }));
        }
        CompletableFuture.allOf(cfs.toArray(new CompletableFuture[cfs.size()])).join();
    }
}
