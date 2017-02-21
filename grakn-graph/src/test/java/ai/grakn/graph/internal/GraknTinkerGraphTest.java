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
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static ai.grakn.util.ErrorMessage.CLOSED_CLEAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class GraknTinkerGraphTest extends GraphTestBase{

    @Test
    public void testMultithreading(){
        Set<Future> futures = new HashSet<>();
        ExecutorService pool = Executors.newFixedThreadPool(10);

        for(int i = 0; i < 100; i ++){
            futures.add(pool.submit(this::addEntityType));
        }

        futures.forEach(future -> {
            try {
                future.get();   
            } catch (InterruptedException | ExecutionException ignored) {
                ignored.printStackTrace();
            }
        });
        assertEquals(108, graknGraph.getTinkerPopGraph().traversal().V().toList().size());
    }
    private void addEntityType(){
        try(GraknGraph graph = Grakn.factory(Grakn.IN_MEMORY, graknGraph.getKeyspace()).getGraph()){
            graph.putEntityType(UUID.randomUUID().toString());
            graknGraph.commitOnClose();
        } catch (GraknValidationException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTestThreadLocal(){
        ExecutorService pool = Executors.newFixedThreadPool(10);
        Set<Future> futures = new HashSet<>();
        AbstractGraknGraph transcation = graknGraph;
        transcation.putEntityType(UUID.randomUUID().toString());
        assertEquals(9, transcation.getTinkerTraversal().toList().size());

        for(int i = 0; i < 100; i ++){
            futures.add(pool.submit(() -> {
                GraknGraph innerTranscation = Grakn.factory(Grakn.IN_MEMORY, graknGraph.getKeyspace()).getGraph();
                innerTranscation.putEntityType(UUID.randomUUID().toString());
            }));
        }

        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException ignored) {
                ignored.printStackTrace();
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
    public void testCloseStandard() throws GraknValidationException {
        AbstractGraknGraph graph = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, "new graph").getGraph();
        graph.close();

        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(ErrorMessage.GRAPH_PERMANENTLY_CLOSED.getMessage(graph.getKeyspace()));

        graph.putEntityType("thing");
    }

    @Test
    public void testCloseWhenClearing(){
        AbstractGraknGraph graphNormal = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, "new graph").getGraph();
        graphNormal.clear();
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(CLOSED_CLEAR.getMessage());
        graphNormal.getEntityType("thing");
    }

}