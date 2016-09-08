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

import io.mindmaps.MindmapsGraph;
import io.mindmaps.exception.MindmapsValidationException;
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

import static org.junit.Assert.assertEquals;

public class MindmapsTitanGraphTest {
    private static final String TEST_CONFIG = "../conf/test/mindmaps-test.properties";
    private static final String TEST_NAME = "mindmapstest";
    private static final String TEST_URI = "localhost";
    private static final boolean TEST_BATCH_LOADING = false;
    private MindmapsGraph mindmapsGraph;

    @Before
    public void setup(){
        mindmapsGraph = new MindmapsTitanGraphFactory().getGraph(TEST_NAME, TEST_URI, TEST_CONFIG, TEST_BATCH_LOADING);
    }

    @After
    public void cleanup(){
        MindmapsGraph mg = new MindmapsTitanGraphFactory().getGraph(TEST_NAME, TEST_URI, TEST_CONFIG, TEST_BATCH_LOADING);
        mg.clear();
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

        assertEquals(108, mindmapsGraph.getTinkerTraversal().V().toList().size());
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
        mindmapsGraph.putEntityType(UUID.randomUUID().toString());
        assertEquals(9, mindmapsGraph.getTinkerTraversal().V().toList().size());

        for(int i = 0; i < 100; i ++){
            futures.add(pool.submit(() -> {
                MindmapsGraph innerTranscation = this.mindmapsGraph;
                innerTranscation.putEntityType(UUID.randomUUID().toString());
            }));
        }

        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException ignored) {

            }
        });

        assertEquals(9, mindmapsGraph.getTinkerTraversal().V().toList().size());
    }
}