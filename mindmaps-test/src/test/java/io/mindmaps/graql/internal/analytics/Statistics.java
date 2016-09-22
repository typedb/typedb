package io.mindmaps.graql.internal.analytics;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.Mindmaps;
import org.elasticsearch.common.collect.Sets;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import spark.Spark;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static io.mindmaps.IntegrationUtils.graphWithNewKeyspace;
import static io.mindmaps.IntegrationUtils.startTestEngine;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Statistics {

    String keyspace;
    MindmapsGraph graph;
    double delta = 0.000001;

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
        System.out.println("AfterClass Done!!!");
    }

    @Test
    public void testStatistics() throws Exception {
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
        ResourceType resourceType2 = graph.putResourceType("resourceType2", ResourceType.DataType.LONG);
        ResourceType resourceType3 = graph.putResourceType("resourceType3", ResourceType.DataType.LONG);
        ResourceType resourceType4 = graph.putResourceType("resourceType4", ResourceType.DataType.STRING);
        ResourceType resourceType5 = graph.putResourceType("resourceType5", ResourceType.DataType.LONG);
        ResourceType resourceType6 = graph.putResourceType("resourceType6", ResourceType.DataType.DOUBLE);

        Analytics computer;

        graph.commit();
        graph = Mindmaps.factory().getGraph(keyspace);

        // resource-type has no instance
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType1")));
        assertFalse(computer.max().isPresent());
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType2")));
        assertFalse(computer.max().isPresent());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType1")));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType2")));
        assertFalse(computer.min().isPresent());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType1")));
        assertFalse(computer.sum().isPresent());
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType2")));
        assertFalse(computer.sum().isPresent());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType1")));
        assertFalse(computer.mean().isPresent());
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType2")));
        assertFalse(computer.mean().isPresent());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType1")));
        assertFalse(computer.std().isPresent());
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType2")));
        assertFalse(computer.std().isPresent());

        graph.putResource(1.2, resourceType1);
        graph.putResource(1.5, resourceType1);
        graph.putResource(1.8, resourceType1);

        graph.putResource(4L, resourceType2);
        graph.putResource(-1L, resourceType2);
        graph.putResource(0L, resourceType2);

        graph.putResource(6L, resourceType5);
        graph.putResource(7L, resourceType5);
        graph.putResource(8L, resourceType5);

        graph.putResource(7.2, resourceType6);
        graph.putResource(7.5, resourceType6);
        graph.putResource(7.8, resourceType6);

        graph.putResource("a", resourceType4);
        graph.putResource("b", resourceType4);
        graph.putResource("c", resourceType4);

        graph.commit();
        graph = Mindmaps.factory().getGraph(keyspace);

        // test max
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType1")));
        assertEquals(1.8, computer.max().get().doubleValue(), delta);
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType2")));
        assertEquals(4L, computer.max().get());
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType("resourceType1"), graph.getType("resourceType6")));
        assertEquals(7.8, computer.max().get().doubleValue(), delta);
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType("resourceType2"), graph.getType("resourceType5")));
        assertEquals(8L, computer.max().get());

        // test min
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType1")));
        assertEquals(1.2, computer.min().get().doubleValue(), delta);
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType2")));
        assertEquals(-1L, computer.min().get());
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType("resourceType1"), graph.getType("resourceType6")));
        assertEquals(1.2, computer.min().get().doubleValue(), delta);
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType("resourceType2"), graph.getType("resourceType5")));
        assertEquals(-1L, computer.min().get());

        // test sum
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType1")));
        assertEquals(4.5, computer.sum().get().doubleValue(), delta);
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType2")));
        assertEquals(3L, computer.sum().get());
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType("resourceType1"), graph.getType("resourceType6")));
        assertEquals(27.0, computer.sum().get().doubleValue(), delta);
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType("resourceType2"), graph.getType("resourceType5")));
        assertEquals(24L, computer.sum().get());

        // test mean
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType1")));
        assertEquals(1.5, computer.mean().get(), delta);
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType2")));
        assertEquals(1.0, computer.mean().get(), delta);
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType("resourceType1"), graph.getType("resourceType6")));
        assertEquals(4.5, computer.mean().get(), delta);
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType("resourceType2"), graph.getType("resourceType5")));
        assertEquals(4.0, computer.mean().get(), delta);

        // test std
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType1")));
        assertEquals(Math.sqrt(0.06), computer.std().get(), delta);
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType2")));
        assertEquals(Math.sqrt(14.0 / 3.0), computer.std().get(), delta);
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType("resourceType1"), graph.getType("resourceType6")));
        assertEquals(Math.sqrt(9.06), computer.std().get(), delta);
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType("resourceType2"), graph.getType("resourceType5")));
        assertEquals(Math.sqrt(70.0 / 6.0), computer.std().get(), delta);

        // if it's not a resource-type
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("thing")));
        assertInvalidExceptionThrown(computer::max);
        assertInvalidExceptionThrown(computer::min);
        assertInvalidExceptionThrown(computer::mean);
        assertInvalidExceptionThrown(computer::sum);
        assertInvalidExceptionThrown(computer::std);

        // resource-type has no instance
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType3")));
        assertFalse(computer.mean().isPresent());
        assertFalse(computer.max().isPresent());
        assertFalse(computer.min().isPresent());
        assertFalse(computer.sum().isPresent());
        assertFalse(computer.std().isPresent());

        // resource-type has incorrect data type
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("resourceType4")));
        assertExceptionThrown(computer::max);
        assertExceptionThrown(computer::min);
        assertExceptionThrown(computer::mean);
        assertExceptionThrown(computer::sum);
        assertExceptionThrown(computer::std);

        // resource-types have different data types
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType("resourceType1"), graph.getType("resourceType2")));
        assertExceptionThrown(computer::max);
        assertExceptionThrown(computer::min);
        assertExceptionThrown(computer::mean);
        assertExceptionThrown(computer::sum);
        assertExceptionThrown(computer::std);
    }

    private void assertInvalidExceptionThrown(Supplier<Optional> method) {
        boolean exceptionThrown = false;
        try {
            method.get();
        } catch (InvalidConceptTypeException e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    private void assertExceptionThrown(Supplier<Optional> method) {
        boolean exceptionThrown = false;
        try {
            method.get();
        } catch (IllegalStateException | IllegalArgumentException e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }
}
