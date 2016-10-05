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

package io.mindmaps.core;

import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.MindmapsTitanTestBase;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.exception.MindmapsValidationException;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 *
 */
public class ConcurrencyTest extends MindmapsTitanTestBase{
    private final String ROLE_1 = "role1";
    private final String ROLE_2 = "role2";
    private final String ENTITY_TYPE = "Entity Type";
    private final String RELATION_TYPE = "Relation Type";
    private MindmapsGraph graph;

    @Before
    public void setUp() throws InterruptedException, MindmapsValidationException {
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, UUID.randomUUID().toString().replaceAll("-", "")).getGraph();
        createOntology(graph);
    }
    private void createOntology(MindmapsGraph graph) throws MindmapsValidationException {
        RoleType role1 = graph.putRoleType(ROLE_1);
        RoleType role2 = graph.putRoleType(ROLE_2);
        graph.putEntityType(ENTITY_TYPE).playsRole(role1).playsRole(role2);
        graph.putRelationType(RELATION_TYPE).hasRole(role1).hasRole(role2);
        graph.commit();
    }

    @After
    public void cleanGraph() {
        graph.rollback();
        assertEquals(2, graph.getEntityType(ENTITY_TYPE).instances().size());
        assertEquals(1, graph.getRelationType(RELATION_TYPE).instances().size());
        graph.clear();
    }

    @Test
    public void testWritingTheSameDataSequentially() throws MindmapsValidationException {
        writeData();
        assertEquals(2, graph.getEntityType(ENTITY_TYPE).instances().size());
        assertEquals(1, graph.getRelationType(RELATION_TYPE).instances().size());

        for(int i = 0; i < 10; i ++){
            boolean exceptionThrown = false;
            try {
                writeData();
            } catch (Exception e){
                exceptionThrown = true;
            }
            assertFalse(exceptionThrown);
        }
    }
    private void writeData() throws MindmapsValidationException {
        EntityType entityType = graph.getEntityType(ENTITY_TYPE);
        RelationType relationType = graph.getRelationType(RELATION_TYPE);
        RoleType role1 = graph.getRoleType(ROLE_1);
        RoleType role2 = graph.getRoleType(ROLE_2);

        Entity e1 = graph.putEntity("e1", entityType);
        Entity e2 = graph.putEntity("e2", entityType);
        graph.putRelation("relation1",relationType).putRolePlayer(role1,e1).putRolePlayer(role2,e2);

        graph.commit();
    }

    @Test
    public void testWritingTheSameDataConcurrentlyWithRetriesOnFailureAndInitialDataWrite() throws ExecutionException, InterruptedException, MindmapsValidationException {
        writeData();
        concurrentWriteSuper();
    }

    @Ignore //This is ignored because we end up with duplicates. These duplicates will be resolved by removing IDs or a merging process
    @Test
    public void testWritingTheSameDataConcurrentlyWithRetriesOnFailure() throws ExecutionException, InterruptedException {
        concurrentWriteSuper();
    }
    private void concurrentWriteSuper() throws ExecutionException, InterruptedException {
        Set<Future> futures = new HashSet<>();
        ExecutorService pool = Executors.newFixedThreadPool(10);

        for(int i = 0; i < 20; i ++){
            futures.add(pool.submit(this::concurrentWrite));
        }

        int numFinished = 0;
        for (Future future : futures) {
            future.get();
            numFinished ++;
        }

        assertEquals(20, numFinished);
    }

    private void concurrentWrite(){
        final int MAX_FAILURE_COUNT = 10;
        boolean doneWriting = false;
        int failureCount = 0;
        while(!doneWriting){
            try{
                writeData();
                doneWriting = true;
            } catch (Exception e){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                graph.rollback(); //It's interesting that we need this after a validation failure. Maybe we should auto rollback after validation failure

                failureCount++;
                if(failureCount >= MAX_FAILURE_COUNT){
                    throw new RuntimeException("Too many failures", e);
                }
            }
        }
    }

}
