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

import ai.grakn.GraknGraph;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.backgroundtasks.distributed.TaskRunner;
import ai.grakn.engine.backgroundtasks.standalone.StandaloneTaskManager;
import ai.grakn.factory.GraphFactory;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.test.AbstractGraknTest;
import ai.grakn.test.EngineTestBase;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.Before;
import org.junit.Test;

import static ai.grakn.graql.Graql.var;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class GraknEngineRunningTest extends AbstractGraknTest {

    @Before
    public void resetLogs() {
        ((Logger) org.slf4j.LoggerFactory.getLogger(TaskRunner.class)).setLevel(Level.DEBUG);
    }

    @Test
    public void graknEngineRunning() throws Exception {
        boolean running = GraknEngineServer.isRunning();
        assertTrue(running);

        // Check that we've loaded the ontology
        GraknGraph graph = GraphFactory.getInstance().getGraph(ConfigProperties.SYSTEM_GRAPH_NAME);
        assertEquals(1, graph.graql().match(var("x").name("scheduled-task")).execute().size());
    }
    
    @Test
    public void graknEngineNotRunning() throws Exception {
        GraknEngineServer.stopHTTP();
        Thread.sleep(5000);

        boolean running = GraknEngineServer.isRunning();
        assertFalse(running);

        GraknEngineServer.startHTTP();
        Thread.sleep(5000);
    }
}
