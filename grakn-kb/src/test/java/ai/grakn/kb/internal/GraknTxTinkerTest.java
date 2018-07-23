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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.kb.internal;

import ai.grakn.Grakn;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.util.ErrorMessage;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GraknTxTinkerTest extends TxTestBase {

    @Test
    public void whenAddingMultipleConceptToTinkerGraph_EnsureGraphIsMutatedDirectlyNotViaTransaction() throws ExecutionException, InterruptedException {
        Set<Future> futures = new HashSet<>();
        ExecutorService pool = Executors.newFixedThreadPool(10);

        tx.putEntityType("Thing");
        tx.commit();

        for(int i = 0; i < 20; i ++){
            futures.add(pool.submit(this::addRandomEntity));
        }

        for (Future future : futures) {
            future.get();
        }

        tx = Grakn.sessionInMemory(tx.keyspace()).transaction(GraknTxType.WRITE);
        assertEquals(20, tx.getEntityType("Thing").instances().count());
    }
    private synchronized void addRandomEntity(){
        try(GraknTx graph = Grakn.sessionInMemory( tx.keyspace()).transaction(GraknTxType.WRITE)){
            graph.getEntityType("Thing").create();
            graph.commit();
        }
    }

    @Test
    public void whenClearingGraph_EnsureGraphIsClosedAndRealodedWhenNextOpening(){
        tx.putEntityType("entity type");
        assertNotNull(tx.getEntityType("entity type"));
        Grakn.Keyspace.delete(tx.keyspace());
        assertTrue(tx.isClosed());
        tx = Grakn.sessionInMemory(tx.keyspace()).transaction(GraknTxType.WRITE);
        assertNull(tx.getEntityType("entity type"));
        assertNotNull(tx.getMetaEntityType());
    }

    @Test
    public void whenMutatingClosedGraph_Throw() throws InvalidKBException {
        EmbeddedGraknTx<?> graph = Grakn.sessionInMemory(Keyspace.of("newgraph")).transaction(GraknTxType.WRITE);
        graph.close();

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(ErrorMessage.TX_CLOSED_ON_ACTION.getMessage("closed", graph.keyspace()));

        graph.putEntityType("Thingy");
    }
}