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
import ai.grakn.GraknTxType;
import ai.grakn.concept.EntityType;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.graph.internal.GraknTitanGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static ai.grakn.util.ErrorMessage.CLOSED_CLEAR;
import static ai.grakn.util.ErrorMessage.GRAPH_CLOSED_ON_ACTION;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GraknTitanGraphTest extends TitanTestBase{
    private GraknGraph graknGraph;

    @Before
    public void setup(){
        graknGraph = titanGraphFactory.open(GraknTxType.WRITE);
    }

    @After
    public void cleanup(){
        if(!graknGraph.isClosed())
            graknGraph.clear();
    }

    @Test
    public void whenCreatingIndependentMutatingTransactionsConcurrently_TheGraphIsUpdatedSafely() throws ExecutionException, InterruptedException {
        Set<Future> futures = new HashSet<>();
        ExecutorService pool = Executors.newFixedThreadPool(40);

        EntityType type = graknGraph.putEntityType("A Type");
        graknGraph.commit();

        for(int i = 0; i < 100; i ++){
            futures.add(pool.submit(() -> addEntity(type)));
        }

        for (Future future : futures) {
            future.get();
        }

        graknGraph = titanGraphFactory.open(GraknTxType.WRITE);
        assertEquals(109, graknGraph.admin().getTinkerTraversal().toList().size());
    }
    private void addEntity(EntityType type){
        GraknTitanGraph graph = titanGraphFactory.open(GraknTxType.WRITE);
        type.addEntity();
        graph.commit();
    }

    @Test
    public void whenAbortingTransaction_ChangesNotCommitted(){
        String label = "My New Type";
        graknGraph.putEntityType(label);
        graknGraph.abort();
        graknGraph = titanGraphFactory.open(GraknTxType.WRITE);
        assertNull(graknGraph.getEntityType(label));
    }

    @Test
    public void whenAbortingTransaction_GraphIsClosedBecauseOfAbort(){
        graknGraph.abort();
        assertTrue("Aborting transaction did not close the graph", graknGraph.isClosed());
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(GRAPH_CLOSED_ON_ACTION.getMessage("closed", graknGraph.getKeyspace()));
        graknGraph.putEntityType("This should fail");
    }

    @Test
    public void testCaseSensitiveKeyspaces(){
        TitanInternalFactory factory1 =  new TitanInternalFactory("case", Grakn.IN_MEMORY, TEST_PROPERTIES);
        TitanInternalFactory factory2 = new TitanInternalFactory("Case", Grakn.IN_MEMORY, TEST_PROPERTIES);
        GraknTitanGraph case1 = factory1.open(GraknTxType.WRITE);
        GraknTitanGraph case2 = factory2.open(GraknTxType.WRITE);

        assertEquals(case1.getKeyspace(), case2.getKeyspace());
    }

    @Test
    public void testClearTitanGraph(){
        GraknTitanGraph graph = new TitanInternalFactory("case", Grakn.IN_MEMORY, TEST_PROPERTIES).open(GraknTxType.WRITE);
        graph.clear();
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(CLOSED_CLEAR.getMessage());
        graph.getEntityType("thing");
    }

    @Test
    public void testPermanentlyClosedGraph(){
        GraknTitanGraph graph = new TitanInternalFactory("test", Grakn.IN_MEMORY, TEST_PROPERTIES).open(GraknTxType.WRITE);

        String entityTypeLabel = "Hello";

        graph.putEntityType(entityTypeLabel);
        assertNotNull(graph.getEntityType(entityTypeLabel));

        graph.close();

        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(GRAPH_CLOSED_ON_ACTION.getMessage("closed", graph.getKeyspace()));

        graph.getEntityType(entityTypeLabel);
    }
}