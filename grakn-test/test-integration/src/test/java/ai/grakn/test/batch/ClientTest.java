/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.test.batch;

import ai.grakn.Grakn;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.batch.Client;
import ai.grakn.engine.KeyspaceStore;
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
        try(GraknTx tx = engine.factory().tx(KeyspaceStore.SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)){
            assertNotNull(tx.getAttributeType(KeyspaceStore.KEYSPACE_RESOURCE.getValue()));
        }
    }

    @Test
    public void whenGraknEngineIsNotRunningOnSpecifiedURI_ClientCannotConnect() throws Exception {
        boolean running = Client.serverIsRunning(Grakn.DEFAULT_URI);
        assertFalse(running);
    }
}
