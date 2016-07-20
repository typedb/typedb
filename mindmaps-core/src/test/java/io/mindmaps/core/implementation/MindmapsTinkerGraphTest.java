package io.mindmaps.core.implementation;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.exceptions.GraphRuntimeException;
import io.mindmaps.core.exceptions.MindmapsValidationException;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MindmapsTinkerGraphTest {
    private MindmapsGraph mindmapsGraph;

    @Before
    public void setup() throws MindmapsValidationException {
        mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        mindmapsGraph.newTransaction().commit();
    }

    @Test
    public void testFakeTransactionHandling() throws Exception {
        MindmapsTransaction mindmapsTransaction = mindmapsGraph.newTransaction();

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

        mindmapsTransaction = mindmapsGraph.newTransaction();
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