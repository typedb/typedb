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

import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.tasks.manager.StandaloneTaskManager;
import ai.grakn.engine.tasks.manager.multiqueue.MultiQueueTaskManager;
import ai.grakn.engine.tasks.manager.singlequeue.SingleQueueTaskManager;
import ai.grakn.engine.util.ConfigProperties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.engine.util.ConfigProperties.TASK_MANAGER_IMPLEMENTATION;
import static ai.grakn.engine.util.ConfigProperties.ZK_CONNECTION_TIMEOUT;
import static junit.framework.TestCase.assertTrue;

public class GraknEngineServerTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testInMemoryMain() throws Exception {
        // Should start engine with in-memory server
        ConfigProperties.getInstance().setConfigProperty(TASK_MANAGER_IMPLEMENTATION, StandaloneTaskManager.class.getName());

        try (GraknEngineServer server = GraknEngineServer.mainWithServer()) {
            assertTrue(server.getTaskManager() instanceof StandaloneTaskManager);
        }
    }

    @Test
    public void testDistributedMultiQueueMain() {
        // Should start engine with distributed server, which means we will get a cannot
        // connect to Zookeeper exception (that has not been started)
        ConfigProperties.getInstance().setConfigProperty(ZK_CONNECTION_TIMEOUT, "1000");
        ConfigProperties.getInstance().setConfigProperty(TASK_MANAGER_IMPLEMENTATION, MultiQueueTaskManager.class.getName());

        exception.expect(RuntimeException.class);
        exception.expectMessage("Could not connect to zookeeper");
        GraknEngineServer.main(new String[]{});
    }

    @Test
    public void testDistributedSingleQueueMain() {
        // Should start engine with distributed server, which means we will get a cannot
        // connect to Zookeeper exception (that has not been started)
        ConfigProperties.getInstance().setConfigProperty(ZK_CONNECTION_TIMEOUT, "1000");
        ConfigProperties.getInstance().setConfigProperty(TASK_MANAGER_IMPLEMENTATION, SingleQueueTaskManager.class.getName());

        exception.expect(RuntimeException.class);
        exception.expectMessage("Could not connect to zookeeper");
        GraknEngineServer.main(new String[]{});
    }
}
