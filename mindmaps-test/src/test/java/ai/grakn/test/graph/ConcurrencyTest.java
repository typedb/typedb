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

package ai.grakn.test.graph;

import ai.grakn.concept.Entity;
import ai.grakn.concept.RelationType;
import ai.grakn.test.AbstractRollbackGraphTest;
import ai.grakn.MindmapsGraph;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.MindmapsValidationException;
import ai.grakn.test.AbstractRollbackGraphTest;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeFalse;

/**
 *
 */
public class ConcurrencyTest extends AbstractRollbackGraphTest {
    private final static String ROLE_1 = "role1";
    private final static String ROLE_2 = "role2";
    private final static String ENTITY_TYPE = "Entity Type";
    private final static String RELATION_TYPE = "Relation Type";

    static void createOntology(MindmapsGraph graph) throws MindmapsValidationException {
        RoleType role1 = graph.putRoleType(ROLE_1);
        RoleType role2 = graph.putRoleType(ROLE_2);
        graph.putEntityType(ENTITY_TYPE).playsRole(role1).playsRole(role2);
        graph.putRelationType(RELATION_TYPE).hasRole(role1).hasRole(role2);
        graph.commit();
    }

    private static void assertResults(MindmapsGraph graph) {
        assertEquals(2, graph.getEntityType(ENTITY_TYPE).instances().size());
        assertEquals(1, graph.getRelationType(RELATION_TYPE).instances().size());
    }

    @Ignore //TODO: Change this test. This was ignored because duplicates can no longer be created via putInstance methods
    @Test
    public void testWritingTheSameDataSequentially() throws MindmapsValidationException, InterruptedException {
        createOntology(graph);
        writeData(graph);
        assertEquals(2, graph.getEntityType(ENTITY_TYPE).instances().size());
        assertEquals(1, graph.getRelationType(RELATION_TYPE).instances().size());

        for(int i = 0; i < 10; i ++){
            boolean exceptionThrown = false;
            try {
                writeData(graph);
            } catch (Exception e){
                exceptionThrown = true;
            }
            assertFalse(exceptionThrown);
        }
        assertResults(graph);
    }
    private static void writeData(MindmapsGraph graph) throws MindmapsValidationException {
        EntityType entityType = graph.getEntityType(ENTITY_TYPE);
        RelationType relationType = graph.getRelationType(RELATION_TYPE);
        RoleType role1 = graph.getRoleType(ROLE_1);
        RoleType role2 = graph.getRoleType(ROLE_2);

        Entity e1 = graph.addEntity(entityType);
        Entity e2 = graph.addEntity(entityType);
        graph.addRelation(relationType).putRolePlayer(role1,e1).putRolePlayer(role2,e2);

        graph.commit();
    }

    @Ignore //TODO: Change this test. This was ignored because duplicates can no longer be created via putInstance methods
    @Test
    public void testWritingTheSameDataConcurrentlyWithRetriesOnFailureAndInitialDataWrite()  throws ExecutionException, InterruptedException, MindmapsValidationException {
        // TODO: Fix this test in tinkergraph
        assumeFalse(usingTinker());

        // TODO: Fix this test in orientdb
        assumeFalse(usingOrientDB());

        createOntology(graph);
        writeData(graph);
        concurrentWriteSuper(graph);
        assertResults(graph);
    }

    @Ignore // TODO: Fix this test
    @Test
    public void testWritingTheSameDataConcurrentlyWithRetriesOnFailure() throws ExecutionException, InterruptedException, MindmapsValidationException {
        createOntology(graph);
        concurrentWriteSuper(graph);
        assertResults(graph);
    }
    private static void concurrentWriteSuper(MindmapsGraph graph) throws ExecutionException, InterruptedException {
        Set<Future> futures = new HashSet<>();
        ExecutorService pool = Executors.newFixedThreadPool(10);

        for(int i = 0; i < 20; i ++){
            futures.add(pool.submit(() -> concurrentWrite(graph)));
        }

        int numFinished = 0;
        for (Future future : futures) {
            future.get();
            numFinished ++;
        }

        assertEquals(20, numFinished);
    }

    private static void concurrentWrite(MindmapsGraph graph){
        final int MAX_FAILURE_COUNT = 10;
        boolean doneWriting = false;
        int failureCount = 0;
        while(!doneWriting){
            try{
                writeData(graph);
                doneWriting = true;
            } catch (Exception e){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                failureCount++;
                if(failureCount >= MAX_FAILURE_COUNT){
                    throw new RuntimeException("Too many failures", e);
                }
            }
        }
    }

}
