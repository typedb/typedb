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
import ai.grakn.engine.backgroundtasks.standalone.StandaloneTaskManager;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.graph.EngineGraknGraph;
import ai.grakn.test.EngineContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.engine.util.ConfigProperties.DISTRIBUTED_TASK_MANAGER;
import static ai.grakn.graql.Graql.var;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class GraknEngineServerTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void graknEngineRunning() throws Throwable {
        EngineContext engine = EngineContext.startDistributedServer();
        engine.before();

        boolean running = GraknEngineServer.isRunning();
        assertTrue(running);

        // Check that we've loaded the ontology
        EngineGraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME);
        assertEquals(1, graph.graql().match(var("x").name("scheduled-task")).execute().size());

        engine.after();
    }
    
    @Test
    public void graknEngineNotRunning() throws Exception {
        boolean running = GraknEngineServer.isRunning();
        assertFalse(running);
    }

    @Test
    public void testInMemoryMain() throws Exception {
        // Should start engine with in-memory server
        ConfigProperties.getInstance().setConfigProperty(DISTRIBUTED_TASK_MANAGER, "false");

        GraknEngineServer.main(new String[]{});
        assertTrue(GraknEngineServer.getTaskManager() instanceof StandaloneTaskManager);
        GraknEngineServer.stop();
    }

    @Test
    public void testDistributedMain() {
        // Should start engine with distributed server, which means we will get a cannot
        // connect to Zookeeper exception (that has not been started)
        ConfigProperties.getInstance().setConfigProperty(DISTRIBUTED_TASK_MANAGER, "true");

        exception.expect(RuntimeException.class);
        exception.expectMessage("Could not connect to zookeeper");
        GraknEngineServer.main(new String[]{});
    }
}
