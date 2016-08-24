package io.mindmaps.graql.internal.analytics;

import io.mindmaps.MindmapsTransaction;
import io.mindmaps.api.CommitLogController;
import io.mindmaps.api.GraphFactoryController;
import io.mindmaps.core.Data;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsClient;
import io.mindmaps.graql.ComputeQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.QueryParser;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static io.mindmaps.IntegrationUtils.hideLogs;
import static io.mindmaps.IntegrationUtils.startTestEngine;
import static io.mindmaps.graql.Graql.*;
import static org.junit.Assert.*;

public class GraqlTest {

    String TEST_KEYSPACE = "mindmapstest";
    MindmapsGraph graph;
    MindmapsTransaction transaction;
    private QueryParser qp;
    private QueryBuilder qb;

    // concepts
    EntityType thing;
    Entity entity1;
    Entity entity2;
    Entity entity3;
    Entity entity4;
    RoleType relation1;
    RoleType relation2;
    RelationType related;

    @BeforeClass
    public static void startController() throws Exception {
        hideLogs();
        startTestEngine();
    }

    @Before
    public void setUp() throws InterruptedException {
        graph = MindmapsClient.getGraph(TEST_KEYSPACE);
        graph.clear();
        graph = MindmapsClient.getGraph(TEST_KEYSPACE);
        transaction = graph.getTransaction();
        qp = QueryParser.create(transaction);
        qb = withTransaction(transaction);
    }

    @After
    public void cleanGraph() {
        graph.clear();
    }

    @Test
    public void testGraqlCount() throws MindmapsValidationException, InterruptedException, ExecutionException {

        // assert the graph is empty
        Analytics computer = new Analytics(TEST_KEYSPACE);
        assertEquals(0, computer.count());

        // create 3 instances
        EntityType thing = transaction.putEntityType("thing");
        transaction.putEntity("1", thing);
        transaction.putEntity("2", thing);
        transaction.putEntity("3", thing);
        transaction.commit();

        long graqlCount = qb.match(
                var("x").isa(var("y")),
                or(var("y").isa("entity-type"), var("y").isa("resource-type"), var("y").isa("relation-type"))
        ).stream().count();

        long computeCount = ((Long) ((ComputeQuery) qp.parseQuery("compute count()")).execute(graph));

        assertEquals(graqlCount, computeCount);
        assertEquals(3L, computeCount);
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

        // compute degrees
        Map<Instance, Long> degrees = ((Map) ((ComputeQuery) qp.parseQuery("compute degrees()")).execute(graph));

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

        assertFalse(degrees.isEmpty());
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

        // compute degrees
        ((ComputeQuery) qp.parseQuery("compute degreesAndPersist()")).execute(graph);

        // assert persisted degrees are correct
        instantiateSimpleConcepts();

        Map<Instance, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(entity4, 1l);
        correctDegrees.put(transaction.getRelation(id1), 2l);
        correctDegrees.put(transaction.getRelation(id2), 2l);
        correctDegrees.put(transaction.getRelation(id3), 2l);

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
}
