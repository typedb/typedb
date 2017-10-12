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

package ai.grakn.test.client;

import ai.grakn.Grakn;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.client.Client;
import ai.grakn.engine.SystemKeyspace;
import ai.grakn.test.EngineContext;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class ClientTest {

    @Test
    public void graknEngineRunning() throws Throwable {
        EngineContext engine = EngineContext.createWithInMemoryRedis();
        engine.before();

        boolean running = Client.serverIsRunning(engine.uri());
        assertTrue(running);

        // Check that we've loaded the schema
        try(GraknTx graph = engine.server().factory().tx(SystemKeyspace.SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)){
            assertNotNull(graph.getAttributeType(SystemKeyspace.KEYSPACE_RESOURCE.getValue()));
        }

        engine.after();
    }

    @Test
    public void graknEngineNotRunning() throws Exception {
        boolean running = Client.serverIsRunning(Grakn.DEFAULT_URI);
        assertFalse(running);
    }
}
