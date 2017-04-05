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

package ai.grakn.client;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.test.EngineContext;
import org.junit.Test;

import static ai.grakn.graql.Graql.var;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ClientTest {

    @Test
    public void graknEngineRunning() throws Throwable {
        EngineContext engine = EngineContext.startInMemoryServer();
        engine.before();

        boolean running = Client.serverIsRunning(Grakn.DEFAULT_URI);
        assertTrue(running);

        // Check that we've loaded the ontology
        try(GraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME, GraknTxType.WRITE)){
            assertEquals(1, graph.graql().match(var("x").label("scheduled-task")).execute().size());
        }

        engine.after();
    }

    @Test
    public void graknEngineNotRunning() throws Exception {
        boolean running = Client.serverIsRunning(Grakn.DEFAULT_URI);
        assertFalse(running);
    }
}
