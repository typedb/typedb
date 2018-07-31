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

package ai.grakn;

import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.kb.internal.GraknTxTinker;
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

public class GraknTest {

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

}