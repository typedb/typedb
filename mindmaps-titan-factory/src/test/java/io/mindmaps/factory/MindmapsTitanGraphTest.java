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

package io.mindmaps.factory;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.exceptions.GraphRuntimeException;
import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.core.implementation.MindmapsTransactionImpl;
import io.mindmaps.core.model.EntityType;
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

import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class MindmapsTitanGraphTest {
    private final String TEST_NAME = "mindmapstest";
    private final String TEST_URI = "localhost";
    private MindmapsGraph mindmapsGraph;

    @Before
    public void setup(){
        cleanup();
        mindmapsGraph = new MindmapsTitanGraphFactory().getGraph(TEST_NAME, TEST_URI, null);
    }

    @After
    public void cleanup(){
        MindmapsGraph mg = new MindmapsTitanGraphFactory().getGraph(TEST_NAME, TEST_URI, null);
        mg.clear();
    }

    @Test
    public void testTransactionHandling() throws Exception {
        MindmapsTransaction mindmapsTransaction = mindmapsGraph.newTransaction();

        mindmapsTransaction.putEntityType("1");
        mindmapsTransaction.close();

        boolean thrown = false;
        try {
            mindmapsTransaction.getEntityType("1");
        } catch (GraphRuntimeException e){
            assertEquals(ErrorMessage.CLOSED.getMessage(mindmapsTransaction.getClass().getName()), e.getMessage());
            thrown = true;
        }
        assertTrue(thrown);

        mindmapsTransaction = mindmapsGraph.newTransaction();
        assertNull(mindmapsTransaction.getConcept("1"));
        EntityType entityType = mindmapsTransaction.putEntityType("1");
        mindmapsTransaction.commit();

        MindmapsTransaction mindmapsTransaction2 = mindmapsGraph.newTransaction();
        assertEquals(entityType, mindmapsTransaction2.getEntityType("1"));
        mindmapsGraph.close();

        thrown = false;
        try {
            mindmapsTransaction.getEntityType("1");
        } catch (IllegalStateException e){
            thrown = true;
        }
        assertTrue(thrown);

        thrown = false;
        try{
            mindmapsGraph.newTransaction();
        } catch (GraphRuntimeException e){
            thrown = true;
            assertEquals(ErrorMessage.CLOSED.getMessage(mindmapsGraph), e.getMessage());
        }
        assertTrue(thrown);
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
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        MindmapsTransactionImpl transaction = (MindmapsTransactionImpl) mindmapsGraph.newTransaction();
        assertEquals(108, transaction.getTinkerPopGraph().traversal().V().toList().size());
    }
    private void addEntityType(MindmapsGraph mindmapsGraph){
        MindmapsTransaction mindmapsTransaction = mindmapsGraph.newTransaction();
        mindmapsTransaction.putEntityType(UUID.randomUUID().toString());
        try {
            mindmapsTransaction.commit();
        } catch (MindmapsValidationException e) {
            e.printStackTrace();
        }
    }
}