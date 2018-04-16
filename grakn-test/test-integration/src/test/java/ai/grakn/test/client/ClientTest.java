/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.test.client;

import ai.grakn.Grakn;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.client.Client;
import ai.grakn.engine.GraknKeyspaceStore;
import ai.grakn.test.rule.EngineContext;
import org.junit.ClassRule;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class ClientTest {
    @ClassRule
    public static final EngineContext engine = EngineContext.create();

    @Test
    public void whenGraknEngineIsRunning_ClientCanConnect() throws Throwable {
        boolean running = Client.serverIsRunning(engine.uri());
        assertTrue(running);

        // Check that we've loaded the schema
        try(GraknTx tx = engine.factory().tx(GraknKeyspaceStore.SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)){
            assertNotNull(tx.getAttributeType(GraknKeyspaceStore.KEYSPACE_RESOURCE.getValue()));
        }
    }

    @Test
    public void whenGraknEngineIsNotRunningOnSpecifiedURI_ClientCannotConnect() throws Exception {
        boolean running = Client.serverIsRunning(Grakn.DEFAULT_URI);
        assertFalse(running);
    }
}
