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

package grakn.core;

import grakn.core.factory.EmbeddedGraknSession;
import grakn.core.kb.internal.EmbeddedGraknTx;
import grakn.core.kb.internal.GraknTxTinker;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class InMemorySessionTest {

    @Test
    public void testInMemory(){
        assertThat(EmbeddedGraknSession.inMemory("test").transaction(GraknTxType.WRITE), instanceOf(GraknTxTinker.class));
    }

    @Test
    public void testInMemorySingleton(){
        GraknTx test1 = EmbeddedGraknSession.inMemory("test1").transaction(GraknTxType.WRITE);
        test1.close();
        GraknTx test11 = EmbeddedGraknSession.inMemory("test1").transaction(GraknTxType.WRITE);
        GraknTx test2 = EmbeddedGraknSession.inMemory("test2").transaction(GraknTxType.WRITE);

        assertEquals(test1, test11);
        assertNotEquals(test1, test2);
    }

    @Test
    public void testComputer(){
        GraknComputer computer = EmbeddedGraknSession.inMemory("bob").getGraphComputer();
        assertThat(computer, instanceOf(GraknComputer.class));
    }

    @Test
    public void testSingletonBetweenBatchAndNormalInMemory(){
        String keyspace = "test1";
        EmbeddedGraknTx<?> graph = (EmbeddedGraknTx<?>) EmbeddedGraknSession.inMemory(keyspace).transaction(GraknTxType.WRITE);
        Graph tinkerGraph = graph.getTinkerPopGraph();
        graph.close();
        EmbeddedGraknTx<?> batchGraph = (EmbeddedGraknTx<?>) EmbeddedGraknSession.inMemory(keyspace).transaction(GraknTxType.BATCH);

        assertNotEquals(graph, batchGraph);
        assertEquals(tinkerGraph, batchGraph.getTinkerPopGraph());

        graph.close();
        batchGraph.close();
    }

    @Test
    public void whenGettingSessionForSameKeyspaceFromMultipleThreads_EnsureSingleSessionIsReturned() throws ExecutionException, InterruptedException {
        Keyspace keyspace = Keyspace.of("myspecialkeyspace");
        Set<Future<?>> futures = ConcurrentHashMap.newKeySet();
        Set<GraknSession> sessions = ConcurrentHashMap.newKeySet();
        ExecutorService pool = Executors.newFixedThreadPool(10);
        for(int i =0; i < 50; i ++){
            futures.add(pool.submit(() -> sessions.add(EmbeddedGraknSession.inMemory(keyspace))));
        }
        for (Future<?> future : futures) {
            future.get();
        }
        assertEquals(1, sessions.size());
    }

}