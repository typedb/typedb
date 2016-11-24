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

package ai.grakn.graph.internal;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GraknTinkerGraphTest extends GraphTestBase{

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
            } catch (InterruptedException | ExecutionException ignored) {

            }
        });
        assertEquals(108, ((AbstractGraknGraph<Graph>) graknGraph).getTinkerPopGraph().traversal().V().toList().size());
    }
    private void addEntityType(GraknGraph graknGraph){
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
        AbstractGraknGraph transcation = (AbstractGraknGraph) graknGraph;
        transcation.putEntityType(UUID.randomUUID().toString());
        assertEquals(9, transcation.getTinkerTraversal().toList().size());

        for(int i = 0; i < 100; i ++){
            futures.add(pool.submit(() -> {
                GraknGraph innerTranscation = graknGraph;
                innerTranscation.putEntityType(UUID.randomUUID().toString());
            }));
        }

        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException ignored) {

            }
        });

        assertEquals(109, transcation.getTinkerTraversal().toList().size()); //This is due to tinkergraphs not being thread local
    }

    @Test
    public void testClear(){
        graknGraph.putEntityType("entity type");
        assertNotNull(graknGraph.getEntityType("entity type"));
        graknGraph.clear();
        graknGraph = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, graknGraph.getKeyspace()).getGraph();
        assertNull(graknGraph.getEntityType("entity type"));
        assertNotNull(graknGraph.getMetaEntityType());
    }

    @Test
    public void testCommitted() throws GraknValidationException {
        assertFalse(graknGraph.hasCommitted());
        graknGraph.putEntityType("Thing");
        graknGraph.commit();
        assertTrue(graknGraph.hasCommitted());
    }

    @Test
    public void testCloseStandard(){
        AbstractGraknGraph graph = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, "new graph").getGraph();
        graph.close();

        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.CLOSED_USER.getMessage())
        ));

        graph.putEntityType("thing");
    }

    @Test
    public void testCloseWhenSwitchingBetweenBatchAndNormal() throws GraknValidationException {
        AbstractGraknGraph graphNormal = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, "new graph").getGraph();
        AbstractGraknGraph graphBatch = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, "new graph").getGraphBatchLoading();

        graphBatch.commit();

        //We get this so we force the other refernce to be invalidated
        AbstractGraknGraph graphNormal2 = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, "new graph").getGraph();

        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.CLOSED_FACTORY.getMessage())
        ));

        graphNormal.getEntityType("thing");
    }

    @Test
    public void testCloseWhenClearing(){
        AbstractGraknGraph graphNormal = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, "new graph").getGraph();
        graphNormal.clear();
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.CLOSED_CLEAR.getMessage())
        ));
        graphNormal.getEntityType("thing");
    }

}