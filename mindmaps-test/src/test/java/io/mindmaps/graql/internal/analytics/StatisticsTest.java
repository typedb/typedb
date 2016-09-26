package io.mindmaps.graql.internal.analytics;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.*;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.factory.MindmapsClient;
import org.elasticsearch.common.collect.Sets;
import org.javatuples.Pair;
import org.junit.*;
import spark.Spark;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Supplier;

import static io.mindmaps.IntegrationUtils.graphWithNewKeyspace;
import static io.mindmaps.IntegrationUtils.startTestEngine;
import static org.junit.Assert.*;

public class StatisticsTest {

    String keyspace;
    MindmapsGraph graph;
    Analytics computer;
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
        System.out.println("After Done!!!");
    }

    @AfterClass
    public static void stop() {
        Spark.stop();
        System.out.println("AfterClass Done!!!");
    }

    @Ignore
    @Test
    public void testStatistics() throws Exception {
        buildTestGraphWithoutResource();

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

        addResources();

        // resource has no owner
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
        assertExceptionThrown(computer::max);
        assertExceptionThrown(computer::min);
        assertExceptionThrown(computer::mean);
        assertExceptionThrown(computer::sum);
        assertExceptionThrown(computer::std);

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

    private void assertExceptionThrown(Supplier<Optional> method) {
        boolean exceptionThrown = false;
        try {
            method.get();
        } catch (IllegalStateException e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    private void buildTestGraphWithoutResource() throws MindmapsValidationException {
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

        graph.addRelation(related)
                .putRolePlayer(relation1, entity1)
                .putRolePlayer(relation2, entity2);
        graph.addRelation(related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity3);
        graph.addRelation(related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity4);

        ResourceType resourceType1 = graph.putResourceType("resourceType1", ResourceType.DataType.DOUBLE);
        ResourceType resourceType2 = graph.putResourceType("resourceType2", ResourceType.DataType.LONG);
        ResourceType resourceType3 = graph.putResourceType("resourceType3", ResourceType.DataType.LONG);
        ResourceType resourceType4 = graph.putResourceType("resourceType4", ResourceType.DataType.STRING);
        ResourceType resourceType5 = graph.putResourceType("resourceType5", ResourceType.DataType.LONG);
        ResourceType resourceType6 = graph.putResourceType("resourceType6", ResourceType.DataType.DOUBLE);

        graph.commit();
        graph = MindmapsClient.getGraph(keyspace);
    }

    private void addResources() throws MindmapsValidationException {
        graph = MindmapsClient.getGraph(keyspace);

        graph.putResource(1.2, graph.getResourceType("resourceType1"));
        graph.putResource(1.5, graph.getResourceType("resourceType1"));
        graph.putResource(1.8, graph.getResourceType("resourceType1"));

        graph.putResource(4L, graph.getResourceType("resourceType2"));
        graph.putResource(-1L, graph.getResourceType("resourceType2"));
        graph.putResource(0L, graph.getResourceType("resourceType2"));

        graph.putResource(6L, graph.getResourceType("resourceType5"));
        graph.putResource(7L, graph.getResourceType("resourceType5"));
        graph.putResource(8L, graph.getResourceType("resourceType5"));

        graph.putResource(7.2, graph.getResourceType("resourceType6"));
        graph.putResource(7.5, graph.getResourceType("resourceType6"));
        graph.putResource(7.8, graph.getResourceType("resourceType6"));

        graph.putResource("a", graph.getResourceType("resourceType4"));
        graph.putResource("b", graph.getResourceType("resourceType4"));
        graph.putResource("c", graph.getResourceType("resourceType4"));

        graph.commit();
        graph = MindmapsClient.getGraph(keyspace);
    }
}
