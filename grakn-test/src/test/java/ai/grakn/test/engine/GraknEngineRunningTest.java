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
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.factory.GraphFactory;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.test.EngineContext;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import info.batey.kafka.unit.Zookeeper;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.engine.GraknEngineServer.startCluster;
import static ai.grakn.engine.GraknEngineServer.stopCluster;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.test.GraknTestEnv.hideLogs;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class GraknEngineRunningTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void resetLogs() {
        hideLogs();
        ((Logger) org.slf4j.LoggerFactory.getLogger(TaskRunner.class)).setLevel(Level.DEBUG);
    }

    @Test
    public void graknEngineRunning() throws Exception {
        startCluster();

        boolean running = GraknEngineServer.isRunning();
        assertTrue(running);

        // Check that we've loaded the ontology
        GraknGraph graph = GraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME);
        assertEquals(1, graph.graql().match(var("x").name("scheduled-task")).execute().size());

        stopCluster();
    }
    
    @Test
    public void graknEngineNotRunning() throws Exception {
        boolean running = GraknEngineServer.isRunning();
        assertFalse(running);
    }

    @Test
    public void failWhenCannotConnectToKafka() throws Exception {
        Zookeeper zookeeper = new Zookeeper(2181);
        zookeeper.startup();

        exception.expect(GraknEngineServerException.class);
        exception.expectMessage(containsString("Could not connect to Kafka."));

        startCluster();
        stopCluster();
    }

    @Test
    public void failWhenCannotConnectToZookeeperTest() throws Exception {
        exception.expect(GraknEngineServerException.class);
        exception.expectMessage(containsString("Could not connect to Zookeeper."));

        startCluster();
        stopCluster();
    }
}
