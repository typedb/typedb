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

import io.mindmaps.MindmapsGraph;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class MindmapsTinkerGraphTest {
    private MindmapsGraph mindmapsGraph;

    @Before
    public void setup() throws MindmapsValidationException {
        mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        mindmapsGraph.commit();
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

        assertEquals(108, ((AbstractMindmapsGraph<Graph>) mindmapsGraph).getTinkerPopGraph().traversal().V().toList().size());
    }
    private void addEntityType(MindmapsGraph mindmapsGraph){
        mindmapsGraph.putEntityType(UUID.randomUUID().toString());
        try {
            mindmapsGraph.commit();
        } catch (MindmapsValidationException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTestThreadLocal(){
        ExecutorService pool = Executors.newFixedThreadPool(10);
        Set<Future> futures = new HashSet<>();
        AbstractMindmapsGraph transcation = (AbstractMindmapsGraph) mindmapsGraph;
        transcation.putEntityType(UUID.randomUUID().toString());
        assertEquals(9, transcation.getTinkerTraversal().V().toList().size());

        for(int i = 0; i < 100; i ++){
            futures.add(pool.submit(() -> {
                MindmapsGraph innerTranscation = mindmapsGraph;
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
    public void testClear(){
        mindmapsGraph.putEntityType("entity type");
        assertNotNull(mindmapsGraph.getEntityType("entity type"));
        mindmapsGraph.clear();
        assertNull(mindmapsGraph.getEntityType("entity type"));
    }
}