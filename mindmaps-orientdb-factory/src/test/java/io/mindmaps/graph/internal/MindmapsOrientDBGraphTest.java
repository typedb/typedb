package io.mindmaps.graph.internal;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.factory.MindmapsOrientDBGraphFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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

public class MindmapsOrientDBGraphTest {
    private static final String TEST_NAME = "mindmapstest";
    private static final String TEST_URI = "memory";
    private static final boolean TEST_BATCH_LOADING = false;
    private MindmapsGraph mindmapsGraph;

    @Before
    public void setup(){
        mindmapsGraph = new MindmapsOrientDBGraphFactory().getGraph(TEST_NAME, TEST_URI, null, TEST_BATCH_LOADING);
    }

    @After
    public void cleanup(){
        MindmapsGraph mg = new MindmapsOrientDBGraphFactory().getGraph(TEST_NAME, TEST_URI, null, TEST_BATCH_LOADING);
        mg.clear();
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

    @Ignore
    @Test
    public void testRollback() {
        assertNull(mindmapsGraph.getEntityType("X"));
        mindmapsGraph.putEntityType("X");
        assertNotNull(mindmapsGraph.getEntityType("X"));
        mindmapsGraph.rollback();
        assertNull(mindmapsGraph.getEntityType("X"));
    }

}