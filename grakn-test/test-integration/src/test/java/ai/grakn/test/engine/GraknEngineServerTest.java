/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
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

package ai.grakn.test.engine;

import ai.grakn.engine.GraknEngineConfig;
import static ai.grakn.engine.GraknEngineConfig.TASK_MANAGER_IMPLEMENTATION;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.tasks.manager.StandaloneTaskManager;
import ai.grakn.engine.tasks.manager.redisqueue.RedisTaskManager;
import ai.grakn.test.EngineContext;
import ai.grakn.test.GraknTestSetup;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertNotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class GraknEngineServerTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule
    public final EngineContext engineContext = EngineContext.startNoQueue();


    @Test
    public void whenEnginePropertiesIndicatesStandaloneTM_StandaloneTmIsStarted() {
        // Should start engine with in-memory server
        GraknEngineConfig conf = GraknEngineConfig.create();
        conf.setConfigProperty(TASK_MANAGER_IMPLEMENTATION, StandaloneTaskManager.class.getName());

        // Start Engine
        try (GraknEngineServer server = new GraknEngineServer(conf)) {
            server.start();
            assertTrue(server.getTaskManager() instanceof StandaloneTaskManager);
        }
    }

    @Test
    public void whenEnginePropertiesIndicatesSingleQueueTM_SingleQueueTmIsStarted() {
        // Should start engine with distributed server, which means we will get a cannot
        // connect to Zookeeper exception (that has not been started)
        GraknEngineConfig conf = GraknEngineConfig.create();
        conf.setConfigProperty(TASK_MANAGER_IMPLEMENTATION, RedisTaskManager.class.getName());

        // Start Engine
        try (GraknEngineServer server = new GraknEngineServer(conf)) {
            server.start();
            assertThat(server.getTaskManager(), instanceOf(RedisTaskManager.class));
        }
    }

    @Test
    public void whenEngineServerIsStarted_SystemKeyspaceIsLoaded(){
        GraknTestSetup.startCassandraIfNeeded();

        GraknEngineConfig conf = GraknEngineConfig.create();
        try (GraknEngineServer server = new GraknEngineServer(conf)) {
            server.start();
            assertNotNull(server.factory().systemKeyspace());

            // init a random keyspace
            String keyspaceName = "thisisarandomwhalekeyspace";
            server.factory().systemKeyspace().ensureKeyspaceInitialised(keyspaceName);

            assertTrue(server.factory().systemKeyspace().containsKeyspace(keyspaceName));
        }
    }
}
