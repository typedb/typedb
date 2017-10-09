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
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.SystemKeyspace;
import ai.grakn.engine.util.SimpleURI;
import ai.grakn.test.EngineContext;
import ai.grakn.util.MockRedisRule;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class ClientTest {

    @Test
    public void graknEngineRunning() throws Throwable {
        EngineContext engine = EngineContext.inMemoryServer();
        MockRedisRule mockRedis = MockRedisRule.create(new SimpleURI(engine.config().getProperty(GraknEngineConfig.REDIS_HOST)).getPort());
        mockRedis.server().start();
        engine.before();

        boolean running = Client.serverIsRunning(engine.uri());
        assertTrue(running);

        // Check that we've loaded the schema
        try(GraknTx graph = engine.server().factory().tx(SystemKeyspace.SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)){
            assertNotNull(graph.getAttributeType(SystemKeyspace.KEYSPACE_RESOURCE.getValue()));
        }

        engine.after();
        mockRedis.server().stop();
    }

    @Test
    public void graknEngineNotRunning() throws Exception {
        boolean running = Client.serverIsRunning(Grakn.DEFAULT_URI);
        assertFalse(running);
    }
}
