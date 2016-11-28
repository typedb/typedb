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

import ai.grakn.GraknGraph;
import ai.grakn.factory.GraphFactory;
import com.jayway.restassured.RestAssured;
import ai.grakn.engine.util.ConfigProperties;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static ai.grakn.graql.Graql.var;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class GraknEngineRunningTest {

    @Before
    public void setCorrectProperties(){
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY, ConfigProperties.TEST_CONFIG_FILE);
        Properties prop = ConfigProperties.getInstance().getProperties();
        RestAssured.baseURI = "http://" + prop.getProperty("server.host") + ":" + prop.getProperty("server.port");
    }

    @Test
    public void graknEngineRunning() throws InterruptedException {
        GraknEngineServer.start();

        Thread.sleep(5000);

        boolean running = GraknEngineServer.isRunning();
        assertTrue(running);

        // Check that we've loaded the ontology
        GraknGraph graph = GraphFactory.getInstance().getGraph(ConfigProperties.SYSTEM_GRAPH_NAME);
        assertEquals(1, graph.graql().match(var("x").name("scheduled-task")).execute().size());

        GraknEngineServer.stop();
        Thread.sleep(5000);
    }

    @Test
    public void graknEngineNotRunning(){
        boolean running = GraknEngineServer.isRunning();
        assertFalse(running);
    }
}
