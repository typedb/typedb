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

package ai.grakn.factory;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.graph.internal.GraknTitanGraph;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static ai.grakn.util.ErrorMessage.CLOSED_CLEAR;
import static ai.grakn.util.ErrorMessage.GRAPH_PERMANENTLY_CLOSED;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GraknTitanGraphTest extends TitanTestBase{
    private GraknGraph graknGraph;

    @Before
    public void setup(){
        graknGraph = titanGraphFactory.getGraph(TEST_BATCH_LOADING);
    }

    @After
    public void cleanup(){
        if(!graknGraph.isClosed())
            graknGraph.clear();
    }

    @Test
    public void testMultithreading(){
        Set<Future> futures = new HashSet<>();
        ExecutorService pool = Executors.newFixedThreadPool(10);

        for(int i = 0; i < 100; i ++){
            futures.add(pool.submit(() -> addEntityType(graknGraph)));
        }

        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        assertEquals(108, graknGraph.admin().getTinkerTraversal().toList().size());
    }
    private void addEntityType(GraknGraph graknGraph){
        graknGraph.open();
        graknGraph.putEntityType(UUID.randomUUID().toString());
        try {
            graknGraph.commit();
        } catch (GraknValidationException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTestThreadLocal(){
        ExecutorService pool = Executors.newFixedThreadPool(10);
        Set<Future> futures = new HashSet<>();
        graknGraph.putEntityType(UUID.randomUUID().toString());
        assertEquals(9, graknGraph.admin().getTinkerTraversal().toList().size());

        for(int i = 0; i < 100; i ++){
            futures.add(pool.submit(() -> {
                GraknGraph innerTranscation = this.graknGraph;
                innerTranscation.putEntityType(UUID.randomUUID().toString());
            }));
        }

        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException ignored) {

            }
        });

        assertEquals(9, graknGraph.admin().getTinkerTraversal().toList().size());
    }

    @Test
    public void testRollback() {
        assertNull(graknGraph.getEntityType("X"));
        graknGraph.putEntityType("X");
        assertNotNull(graknGraph.getEntityType("X"));
        graknGraph.rollback();
        assertNull(graknGraph.getEntityType("X"));
    }

    @Test
    public void testCaseSensitiveKeyspaces(){
        TitanInternalFactory factory1 =  new TitanInternalFactory("case", Grakn.IN_MEMORY, TEST_PROPERTIES);
        TitanInternalFactory factory2 = new TitanInternalFactory("Case", Grakn.IN_MEMORY, TEST_PROPERTIES);
        GraknTitanGraph case1 = factory1.getGraph(TEST_BATCH_LOADING);
        GraknTitanGraph case2 = factory2.getGraph(TEST_BATCH_LOADING);

        assertEquals(case1.getKeyspace(), case2.getKeyspace());
    }

    @Test
    public void testClearTitanGraph(){
        GraknTitanGraph graph = new TitanInternalFactory("case", Grakn.IN_MEMORY, TEST_PROPERTIES).getGraph(false);
        graph.clear();
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(CLOSED_CLEAR.getMessage());
        graph.getEntityType("thing");
    }

    @Test
    public void testPermanentlyClosedGraph(){
        GraknTitanGraph graph = new TitanInternalFactory("test", Grakn.IN_MEMORY, TEST_PROPERTIES).getGraph(false);

        String entityTypeName = "Hello";

        graph.putEntityType(entityTypeName);
        assertNotNull(graph.getEntityType(entityTypeName));

        graph.close();

        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(GRAPH_PERMANENTLY_CLOSED.getMessage(graph.getKeyspace()));

        graph.open();
    }

    @Test
    public void testStableTransactions() throws GraknValidationException {
        GraknTitanGraph graph = new TitanInternalFactory("stabletransactions", Grakn.IN_MEMORY, TEST_PROPERTIES).getGraph(false);
        assertEquals(1, ((StandardTitanGraph) graph.getTinkerPopGraph()).getOpenTxs());

        graph.putEntityType("name 1");
        graph.commit();
        assertEquals(1, ((StandardTitanGraph) graph.getTinkerPopGraph()).getOpenTxs());

        graph.putEntityType("name 2");
        graph.commit();
        assertEquals(1, ((StandardTitanGraph) graph.getTinkerPopGraph()).getOpenTxs());

        graph.putEntityType("name 3");
        graph.commit();
        assertEquals(1, ((StandardTitanGraph) graph.getTinkerPopGraph()).getOpenTxs());

        graph.putEntityType("name 4");
        graph.commit();
        assertEquals(1, ((StandardTitanGraph) graph.getTinkerPopGraph()).getOpenTxs());
    }
}