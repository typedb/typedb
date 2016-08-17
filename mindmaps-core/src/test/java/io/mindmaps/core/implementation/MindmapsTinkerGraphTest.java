/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core.implementation;

import io.mindmaps.MindmapsTransaction;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.implementation.exception.GraphRuntimeException;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class MindmapsTinkerGraphTest {
    private MindmapsGraph mindmapsGraph;

    @Before
    public void setup() throws MindmapsValidationException {
        mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        mindmapsGraph.getTransaction().commit();
    }

    @Test
    public void testFakeTransactionHandling() throws Exception {
        MindmapsTransaction mindmapsTransaction = mindmapsGraph.getTransaction();

        EntityType entityType = mindmapsTransaction.putEntityType("1");
        mindmapsTransaction.commit();
        mindmapsTransaction.close();

        boolean thrown = false;
        try{
            mindmapsTransaction.putEntityType("2");
        } catch (GraphRuntimeException e){
            assertEquals(ErrorMessage.CLOSED.getMessage(mindmapsTransaction.getClass().getName()), e.getMessage());
            thrown = true;
        }
        assertTrue(thrown);

        mindmapsTransaction = mindmapsGraph.getTransaction();
        assertEquals(entityType, mindmapsTransaction.getConcept("1"));
    }

    @Test
    public void testMultithreading(){
        Set<Future> futures = new HashSet<>();
        ExecutorService pool = Executors.newFixedThreadPool(10);

        for(int i = 0; i < 100; i ++){
            futures.add(pool.submit(() -> addEntityType(mindmapsGraph)));
        }

        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException ignored) {

            }
        });

        MindmapsTransactionImpl transaction = (MindmapsTransactionImpl) mindmapsGraph.getTransaction();
        assertEquals(108, transaction.getTinkerPopGraph().traversal().V().toList().size());
    }
    private void addEntityType(MindmapsGraph mindmapsGraph){
        MindmapsTransaction mindmapsTransaction = mindmapsGraph.getTransaction();
        mindmapsTransaction.putEntityType(UUID.randomUUID().toString());
        try {
            mindmapsTransaction.commit();
        } catch (MindmapsValidationException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSingletonTinkerTransaction() throws ExecutionException, InterruptedException {
        MindmapsTransaction transaction = mindmapsGraph.getTransaction();
        MindmapsTransaction transaction2 = mindmapsGraph.getTransaction();
        final MindmapsTransaction[] transaction3 = new MindmapsTransaction[1];

        assertEquals(transaction, transaction2);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.submit(() -> transaction3[0] = mindmapsGraph.getTransaction()).get();

        assertNotNull(transaction3[0]);
        assertNotEquals(transaction, transaction3[0]);
    }

    @Test
    public void testTestThreadLocal(){
        ExecutorService pool = Executors.newFixedThreadPool(10);
        Set<Future> futures = new HashSet<>();
        MindmapsTransactionImpl transcation = (MindmapsTransactionImpl) mindmapsGraph.getTransaction();
        transcation.putEntityType(UUID.randomUUID().toString());
        assertEquals(9, transcation.getTinkerTraversal().V().toList().size());

        for(int i = 0; i < 100; i ++){
            futures.add(pool.submit(() -> {
                MindmapsTransaction innerTranscation = mindmapsGraph.getTransaction();
                innerTranscation.putEntityType(UUID.randomUUID().toString());
            }));
        }

        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException ignored) {

            }
        });

        assertEquals(109, transcation.getTinkerTraversal().V().toList().size()); //This is due to tinkergraphs not being thread local
    }

    @Test
    public void testDifferentEmptyGraphs(){
        AbstractMindmapsGraph graph1 = (AbstractMindmapsGraph) MindmapsTestGraphFactory.newEmptyGraph();
        AbstractMindmapsGraph graph2 = (AbstractMindmapsGraph) MindmapsTestGraphFactory.newEmptyGraph();

        assertNotEquals(graph1, graph2);
        assertNotEquals(graph1.getGraph(), graph2.getGraph());
        assertNotEquals(graph1.getTransaction(), graph2.getTransaction());
    }
}