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

package io.mindmaps.graql.internal.analytics;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.Data;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsClient;
import org.junit.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class AnalyticsTest {

    String TEST_KEYSPACE = Analytics.keySpace;
    MindmapsGraph graph;
    MindmapsTransaction transaction;

    // concepts
    EntityType thing;
    Entity entity1;
    Entity entity2;
    Entity entity3;
    Entity entity4;
    RoleType relation1;
    RoleType relation2;
    RelationType related;
    RoleType degreeTarget;
    RoleType degreeValue;
    RelationType hasDegree;
    ResourceType degreeResource;

    long startTime;

    @BeforeClass
    public static void startController() throws InterruptedException {
//        new GraphFactoryController();
//        sleep(5000);

        // Disable horrid cassandra logs
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);
    }

    @Before
    public void setUp() throws InterruptedException {
        System.out.println();
        System.out.println("Clearing the graph");
        graph = MindmapsClient.getGraph(TEST_KEYSPACE);
        graph.clear();
        Thread.sleep(5000);
        graph = MindmapsClient.getGraph(TEST_KEYSPACE);
        transaction = graph.getTransaction();
    }

    @After
    public void cleanGraph() {
        graph.clear();
    }

    @Test
    public void testCount() throws Exception {
        // assert the graph is empty
        System.out.println();
        System.out.println("Counting");
        Analytics computer = new Analytics();
        startTime = System.currentTimeMillis();
        Assert.assertEquals(0, computer.count());
        System.out.println();
        System.out.println(System.currentTimeMillis() - startTime + " ms");

        // create 3 instances
        System.out.println();
        System.out.println("Creating 3 instances");
        EntityType thing = transaction.putEntityType("thing");
        transaction.putEntity("1",thing);
        transaction.putEntity("2",thing);
        transaction.putEntity("3",thing);
        transaction.commit();

        Thread.sleep(10000);

        // assert computer returns the correct count of instances
        System.out.println();
        System.out.println("Counting");
        computer = new Analytics();
        startTime = System.currentTimeMillis();
        Assert.assertEquals(3, computer.count());
        System.out.println();
        System.out.println(System.currentTimeMillis() - startTime + " ms");
    }

    @Test
    public void testDegrees() throws Exception {
        instantiateSimpleConcepts();

        // relate them
        String id1 = UUID.randomUUID().toString();
        transaction.putRelation(id1,related)
                .putRolePlayer(relation1, entity1)
                .putRolePlayer(relation2, entity2);

        String id2 = UUID.randomUUID().toString();
        transaction.putRelation(id2,related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity3);

        String id3 = UUID.randomUUID().toString();
        transaction.putRelation(id3,related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity4);

        transaction.commit();

        // compute degrees
        Analytics computer = new Analytics();
        Map<Instance,Long> degrees = computer.degrees();

        // assert degrees are correct
        instantiateSimpleConcepts();
        Map<Instance,Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(entity4, 1l);
        correctDegrees.put(transaction.getRelation(id1),2l);
        correctDegrees.put(transaction.getRelation(id2),2l);
        correctDegrees.put(transaction.getRelation(id3),2l);

        assertTrue(!degrees.isEmpty());
        degrees.entrySet().forEach(degree -> {
            assertTrue(correctDegrees.containsKey(degree.getKey()));
            assertTrue(correctDegrees.get(degree.getKey()).equals(degree.getValue()));
        });
    }

    private void instantiateSimpleConcepts() {

        // create 3 instances
        thing = transaction.putEntityType("thing");
        entity1 = transaction.putEntity("1", thing);
        entity2 = transaction.putEntity("2", thing);
        entity3 = transaction.putEntity("3", thing);
        entity4 = transaction.putEntity("4", thing);

        relation1 = transaction.putRoleType("relation1");
        relation2 = transaction.putRoleType("relation2");
        thing.playsRole(relation1).playsRole(relation2);
        related = transaction.putRelationType("related").hasRole(relation1).hasRole(relation2);

    }

    @Test
    public void testDegreesAndPersist() throws Exception {
        instantiateSimpleConcepts();

        // relate them
        String id1 = UUID.randomUUID().toString();
        transaction.putRelation(id1,related)
                .putRolePlayer(relation1, entity1)
                .putRolePlayer(relation2, entity2);

        String id2 = UUID.randomUUID().toString();
        transaction.putRelation(id2,related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2,entity3);

        String id3 = UUID.randomUUID().toString();
        transaction.putRelation(id3,related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity4);

        // create ontology for persisting
        persistOntology();

        transaction.commit();

        // compute degrees
        Analytics computer = new Analytics();
        computer.degreesAndPersist();

        // assert persisted degrees are correct
        instantiateSimpleConcepts();
        persistOntology();
        Map<Instance,Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(entity4, 1l);
        correctDegrees.put(transaction.getRelation(id1),2l);
        correctDegrees.put(transaction.getRelation(id2),2l);
        correctDegrees.put(transaction.getRelation(id3),2l);

        correctDegrees.entrySet().forEach(degree -> {
            Instance instance = degree.getKey();
            Collection<Resource<?>> resources = null;
            if (instance.isEntity()) {
                resources = instance.asEntity().resources();
            } else if (instance.isRelation()) {
                resources = instance.asRelation().resources();
            }
            assertTrue(resources.iterator().next().getValue().equals(degree.getValue()));
        });
    }

    private void persistOntology() {
        degreeResource = transaction.putResourceType("degree-resource", Data.LONG);
        degreeTarget = transaction.putRoleType("degree-target");
        degreeValue = transaction.putRoleType("degree-value");
        hasDegree = transaction.putRelationType("has-degree")
                .hasRole(degreeTarget).hasRole(degreeValue);
        thing.playsRole(degreeTarget);
        related.playsRole(degreeTarget);
        degreeResource.playsRole(degreeValue);
    }

}