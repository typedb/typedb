package io.mindmaps.graql.internal.analytics;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.*;
import io.mindmaps.factory.MindmapsClient;
import org.javatuples.Pair;
import org.junit.*;
import spark.Spark;

import java.util.Collections;
import java.util.UUID;

import static io.mindmaps.IntegrationUtils.graphWithNewKeyspace;
import static io.mindmaps.IntegrationUtils.startTestEngine;
import static org.junit.Assert.assertEquals;

public class Statistics {

    String keyspace;
    MindmapsGraph graph;

    @BeforeClass
    public static void startController() throws Exception {
        startTestEngine();
    }

    @Before
    public void setUp() throws InterruptedException {
        Pair<MindmapsGraph, String> result = graphWithNewKeyspace();
        graph = result.getValue0();
        keyspace = result.getValue1();
    }

    @After
    public void cleanGraph() {
        graph.clear();
        graph.close();
    }

    @AfterClass
    public static void stop() {
        Spark.stop();
        System.out.println("Done!!!");
    }

    @Test
    public void testMaxAndMin() throws Exception {
        EntityType thing = graph.putEntityType("thing");
        EntityType anotherThing = graph.putEntityType("another");

        Entity entity1 = graph.putEntity("1", thing);
        Entity entity2 = graph.putEntity("2", thing);
        Entity entity3 = graph.putEntity("3", thing);
        Entity entity4 = graph.putEntity("4", thing);

        RoleType relation1 = graph.putRoleType("relation1");
        RoleType relation2 = graph.putRoleType("relation2");
        thing.playsRole(relation1).playsRole(relation2);
        anotherThing.playsRole(relation1).playsRole(relation2);
        RelationType related = graph.putRelationType("related").hasRole(relation1).hasRole(relation2);

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

        ResourceType resourceType1 = graph.putResourceType("resourceType1", ResourceType.DataType.DOUBLE);
        graph.putResource(1.2, resourceType1);
        graph.putResource(1.5, resourceType1);
        graph.putResource(1.8, resourceType1);

        ResourceType resourceType2 = graph.putResourceType("resourceType2", ResourceType.DataType.LONG);
        graph.putResource(Long.MIN_VALUE, resourceType2);
        graph.putResource(-1L, resourceType2);
        graph.putResource(0L, resourceType2);

        graph.commit();

        Analytics computer = new Analytics(keyspace);
        computer.degreesAndPersist();
        graph = MindmapsClient.getGraph(keyspace);

        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType1")));
        assertEquals(1.8, computer.max());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType1")));
        assertEquals(1.2, computer.min());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType2")));
        assertEquals(0L, computer.max());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType2")));
        assertEquals(Long.MIN_VALUE, computer.min());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType("degree")));
        assertEquals(3L, computer.max());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType("degree")));
        assertEquals(0L, computer.min());
    }
}
