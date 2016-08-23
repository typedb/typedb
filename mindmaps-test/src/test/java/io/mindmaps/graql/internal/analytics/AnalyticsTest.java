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
import io.mindmaps.api.CommitLogController;
import io.mindmaps.api.GraphFactoryController;
import io.mindmaps.core.Data;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsClient;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.*;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
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

    long startTime;

    @BeforeClass
    public static void startController() throws InterruptedException, ConfigurationException, IOException, TTransportException {
        // Disable horrid cassandra logs
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);

        EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-embedded.yaml");
        new GraphFactoryController();
        new CommitLogController();

        sleep(5000);
    }

    @AfterClass
    public static void stopController() {
        EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
    }

    @Before
    public void setUp() throws InterruptedException {
        System.out.println();
        System.out.println("Clearing the graph");
        graph = MindmapsClient.getGraph(TEST_KEYSPACE);
        graph.clear();
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
        transaction.putEntity("1", thing);
        transaction.putEntity("2", thing);
        transaction.putEntity("3", thing);
        transaction.commit();

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
        transaction.putRelation(id1, related)
                .putRolePlayer(relation1, entity1)
                .putRolePlayer(relation2, entity2);

        String id2 = UUID.randomUUID().toString();
        transaction.putRelation(id2, related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity3);

        String id3 = UUID.randomUUID().toString();
        transaction.putRelation(id3, related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity4);

        transaction.commit();

        // assert degrees are correct
        instantiateSimpleConcepts();
        Map<Instance, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(entity4, 1l);
        correctDegrees.put(transaction.getRelation(id1), 2l);
        correctDegrees.put(transaction.getRelation(id2), 2l);
        correctDegrees.put(transaction.getRelation(id3), 2l);

        // compute degrees
        Analytics computer = new Analytics();
        Map<Instance, Long> degrees = computer.degrees();

        assertTrue(!degrees.isEmpty());
        degrees.entrySet().forEach(degree -> {
            assertTrue(correctDegrees.containsKey(degree.getKey()));
            assertTrue(correctDegrees.get(degree.getKey()).equals(degree.getValue()));
        });

        // compute degrees again after persisting degrees
        computer.degreesAndPersist();
        Map<Instance, Long> degrees2 = computer.degrees();

        assertEquals(degrees.size(), degrees2.size());

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
        transaction.putRelation(id1, related)
                .putRolePlayer(relation1, entity1)
                .putRolePlayer(relation2, entity2);

        String id2 = UUID.randomUUID().toString();
        transaction.putRelation(id2, related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity3);

        String id3 = UUID.randomUUID().toString();
        transaction.putRelation(id3, related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity4);

        transaction.commit();

        Map<Instance, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(entity4, 1l);
        correctDegrees.put(transaction.getRelation(id1), 2l);
        correctDegrees.put(transaction.getRelation(id2), 2l);
        correctDegrees.put(transaction.getRelation(id3), 2l);

        // compute degrees
        Analytics computer = new Analytics();
        computer.degreesAndPersist();

        // assert persisted degrees are correct
        instantiateSimpleConcepts();
        correctDegrees.entrySet().forEach(degree -> {
            Instance instance = degree.getKey();
            Collection<Resource<?>> resources = null;
            if (instance.isEntity()) {
                resources = instance.asEntity().resources();
            } else if (instance.isRelation()) {
                resources = instance.asRelation().resources();
            }
            assert resources != null;
            assertTrue(resources.iterator().next().getValue().equals(degree.getValue()));
        });

        long numVertices = computer.count();

        // compute again
        computer.degreesAndPersist();

        correctDegrees.entrySet().forEach(degree -> {
            Instance instance = degree.getKey();
            Collection<Resource<?>> resources = null;
            if (instance.isEntity()) {
                resources = instance.asEntity().resources();
            } else if (instance.isRelation()) {
                resources = instance.asRelation().resources();
            }
            assert resources != null;
            assertTrue(resources.iterator().next().getValue().equals(degree.getValue()));
        });

        // assert the number of vertices remain the same
//        assertEquals(numVertices, computer.count());
    }
}