/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

import ai.grakn.graph.internal.MindmapsTitanGraph;
import ai.grakn.MindmapsGraph;
import ai.grakn.exception.MindmapsValidationException;
import ai.grakn.graph.internal.MindmapsTitanGraph;
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

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MindmapsTitanGraphTest {
    private static final String TEST_CONFIG = "conf/mindmaps-titan-test.properties";
    private static final String TEST_NAME = "mindmapstest";
    private static final String TEST_URI = null;
    private static final boolean TEST_BATCH_LOADING = false;
    private MindmapsGraph mindmapsGraph;

    @Before
    public void setup(){
        mindmapsGraph = new TitanInternalFactory(TEST_NAME, TEST_URI, TEST_CONFIG).getGraph(TEST_BATCH_LOADING);
    }

    @After
    public void cleanup(){
        mindmapsGraph.clear();
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

        assertEquals(108, mindmapsGraph.getTinkerTraversal().toList().size());
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
        assertEquals(9, mindmapsGraph.getTinkerTraversal().toList().size());

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

        assertEquals(9, mindmapsGraph.getTinkerTraversal().toList().size());
    }

    @Test
    public void testRollback() {
        assertNull(mindmapsGraph.getEntityType("X"));
        mindmapsGraph.putEntityType("X");
        assertNotNull(mindmapsGraph.getEntityType("X"));
        mindmapsGraph.rollback();
        assertNull(mindmapsGraph.getEntityType("X"));
    }

    @Test
    public void testCaseSensitiveKeyspaces(){
        TitanInternalFactory factory1 = new TitanInternalFactory("case", TEST_URI, TEST_CONFIG);
        TitanInternalFactory factory2 = new TitanInternalFactory("Case", TEST_URI, TEST_CONFIG);
        MindmapsTitanGraph case1 = factory1.getGraph(TEST_BATCH_LOADING);
        MindmapsTitanGraph case2 = factory2.getGraph(TEST_BATCH_LOADING);

        assertEquals(case1.getKeyspace(), case2.getKeyspace());
    }
}