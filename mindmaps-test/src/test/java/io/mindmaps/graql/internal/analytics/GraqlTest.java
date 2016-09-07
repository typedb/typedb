package io.mindmaps.graql.internal.analytics;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.*;
import io.mindmaps.graql.ComputeQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.QueryParser;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static io.mindmaps.IntegrationUtils.graphWithNewKeyspace;
import static io.mindmaps.IntegrationUtils.startTestEngine;
import static io.mindmaps.graql.Graql.*;
import static org.junit.Assert.*;

public class GraqlTest {

    String keyspace = "mindmapstest";
    MindmapsGraph graph;
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
        startTestEngine();
    }

    @Before
    public void setUp() throws InterruptedException {
        Pair<MindmapsGraph, String> result = graphWithNewKeyspace();
        graph = result.getValue0();
        keyspace = result.getValue1();
        qp = QueryParser.create(graph);
        qb = withGraph(graph);
    }

    @After
    public void cleanGraph() {
        graph.clear();
    }

    @Test
    public void testGraqlCount() throws MindmapsValidationException, InterruptedException, ExecutionException {

        // assert the graph is empty
        Analytics computer = new Analytics(keyspace);
        assertEquals(0, computer.count());

        // create 3 instances
        EntityType thing = graph.putEntityType("thing");
        graph.putEntity("1", thing);
        graph.putEntity("2", thing);
        graph.putEntity("3", thing);
        graph.commit();

        long graqlCount = qb.match(
                var("x").isa(var("y")),
                or(var("y").isa("entity-type"), var("y").isa("resource-type"), var("y").isa("relation-type"))
        ).stream().count();

        long computeCount = ((Long) ((ComputeQuery) qp.parseQuery("compute count")).execute(graph));

        assertEquals(graqlCount, computeCount);
        assertEquals(3L, computeCount);
    }

    @Test
    public void testGraqlCountSubgraph() throws Exception {
        // create 3 instances
        thing = graph.putEntityType("thing");
        EntityType anotherThing = graph.putEntityType("another");
        graph.putEntity("1", thing);
        graph.putEntity("2", thing);
        graph.putEntity("3", anotherThing);
        graph.commit();

        long computeCount = ((Long) ((ComputeQuery) qp.parseQuery("compute count in thing, thing")).execute(graph));
        assertEquals(2, computeCount);
    }


    @Test
    public void testDegrees() throws Exception {
        instantiateSimpleConcepts();

        // relate them
        String id1 = UUID.randomUUID().toString();
        graph.putRelation(id1, related)
                .putRolePlayer(relation1, entity1)
                .putRolePlayer(relation2, entity2);

        String id2 = UUID.randomUUID().toString();
        graph.putRelation(id2, related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity3);

        String id3 = UUID.randomUUID().toString();
        graph.putRelation(id3, related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity4);

        graph.commit();

        // compute degrees
        Map<Instance, Long> degrees = ((Map) ((ComputeQuery) qp.parseQuery("compute degrees")).execute(graph));

        // assert degrees are correct
        instantiateSimpleConcepts();
        Map<Instance, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(entity4, 1l);
        correctDegrees.put(graph.getRelation(id1), 2l);
        correctDegrees.put(graph.getRelation(id2), 2l);
        correctDegrees.put(graph.getRelation(id3), 2l);

        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(degree -> {
            assertTrue(correctDegrees.containsKey(degree.getKey()));
            assertTrue(correctDegrees.get(degree.getKey()).equals(degree.getValue()));
        });
    }

    private void instantiateSimpleConcepts() {

        // create 3 instances
        thing = graph.putEntityType("thing");
        entity1 = graph.putEntity("1", thing);
        entity2 = graph.putEntity("2", thing);
        entity3 = graph.putEntity("3", thing);
        entity4 = graph.putEntity("4", thing);

        relation1 = graph.putRoleType("relation1");
        relation2 = graph.putRoleType("relation2");
        thing.playsRole(relation1).playsRole(relation2);
        related = graph.putRelationType("related").hasRole(relation1).hasRole(relation2);

    }

    @Test
    public void testDegreesAndPersist() throws Exception {
        instantiateSimpleConcepts();

        // relate them
        String id1 = UUID.randomUUID().toString();
        graph.putRelation(id1, related)
                .putRolePlayer(relation1, entity1)
                .putRolePlayer(relation2, entity2);

        String id2 = UUID.randomUUID().toString();
        graph.putRelation(id2, related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity3);

        String id3 = UUID.randomUUID().toString();
        graph.putRelation(id3, related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity4);

        graph.commit();

        // compute degrees
        ((ComputeQuery) qp.parseQuery("compute degreesAndPersist")).execute(graph);

        // assert persisted degrees are correct
        instantiateSimpleConcepts();

        Map<Instance, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(entity4, 1l);
        correctDegrees.put(graph.getRelation(id1), 2l);
        correctDegrees.put(graph.getRelation(id2), 2l);
        correctDegrees.put(graph.getRelation(id3), 2l);

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
