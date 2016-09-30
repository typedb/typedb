package io.mindmaps.graql.internal.analytics;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.Mindmaps;
import io.mindmaps.graql.internal.util.GraqlType;
import org.elasticsearch.common.collect.Sets;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import spark.Spark;

import java.util.*;
import java.util.function.Supplier;

import static io.mindmaps.IntegrationUtils.graphWithNewKeyspace;
import static io.mindmaps.IntegrationUtils.startTestEngine;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StatisticsTest {

    private static final String thing = "thing";
    private static final String anotherThing = "anotherThing";

    private static final String resourceType1 = "resourceType1";
    private static final String resourceType2 = "resourceType2";
    private static final String resourceType3 = "resourceType3";
    private static final String resourceType4 = "resourceType4";
    private static final String resourceType5 = "resourceType5";
    private static final String resourceType6 = "resourceType6";


    String keyspace;
    MindmapsGraph graph;
    Analytics computer;
    double delta = 0.000001;

    @BeforeClass
    public static void startController() throws Exception {
        startTestEngine();
    }

    @Before
    public void setUp() throws InterruptedException, MindmapsValidationException {
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
        addOntologyAndEntities();

        // add resource instance
        addResourcesInstances();

        // resource-type has no instance
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType1)));
        assertFalse(computer.max().isPresent());
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType2)));
        assertFalse(computer.max().isPresent());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType1)));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType2)));
        assertFalse(computer.min().isPresent());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType1)));
        assertFalse(computer.sum().isPresent());
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType2)));
        assertFalse(computer.sum().isPresent());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType1)));
        assertFalse(computer.mean().isPresent());
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType2)));
        assertFalse(computer.mean().isPresent());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType1)));
        assertFalse(computer.std().isPresent());
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType2)));
        assertFalse(computer.std().isPresent());

        addResourcesInstances();

        // resource has no owner
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType1)));
        assertFalse(computer.max().isPresent());
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType2)));
        assertFalse(computer.max().isPresent());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType1)));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType2)));
        assertFalse(computer.min().isPresent());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType1)));
        assertFalse(computer.sum().isPresent());
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType2)));
        assertFalse(computer.sum().isPresent());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType1)));
        assertFalse(computer.mean().isPresent());
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType2)));
        assertFalse(computer.mean().isPresent());

        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType1)));
        assertFalse(computer.std().isPresent());
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType2)));
        assertFalse(computer.std().isPresent());

        // test max
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType1)));
        assertEquals(1.8, computer.max().get().doubleValue(), delta);
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType2)));
        assertEquals(4L, computer.max().get());
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType(resourceType1), graph.getType(resourceType6)));
        assertEquals(7.8, computer.max().get().doubleValue(), delta);
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType(resourceType2), graph.getType(resourceType5)));
        assertEquals(8L, computer.max().get());

        // test min
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType1)));
        assertEquals(1.2, computer.min().get().doubleValue(), delta);
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType2)));
        assertEquals(-1L, computer.min().get());
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType(resourceType1), graph.getType(resourceType6)));
        assertEquals(1.2, computer.min().get().doubleValue(), delta);
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType(resourceType2), graph.getType(resourceType5)));
        assertEquals(-1L, computer.min().get());

        // test sum
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType1)));
        assertEquals(4.5, computer.sum().get().doubleValue(), delta);
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType2)));
        assertEquals(3L, computer.sum().get());
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType(resourceType1), graph.getType(resourceType6)));
        assertEquals(27.0, computer.sum().get().doubleValue(), delta);
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType(resourceType2), graph.getType(resourceType5)));
        assertEquals(24L, computer.sum().get());

        // test mean
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType1)));
        assertEquals(1.5, computer.mean().get(), delta);
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType2)));
        assertEquals(1.0, computer.mean().get(), delta);
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType(resourceType1), graph.getType(resourceType6)));
        assertEquals(4.5, computer.mean().get(), delta);
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType(resourceType2), graph.getType(resourceType5)));
        assertEquals(4.0, computer.mean().get(), delta);

        // test std
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType1)));
        assertEquals(Math.sqrt(0.06), computer.std().get(), delta);
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType2)));
        assertEquals(Math.sqrt(14.0 / 3.0), computer.std().get(), delta);
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType(resourceType1), graph.getType(resourceType6)));
        assertEquals(Math.sqrt(9.06), computer.std().get(), delta);
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType(resourceType2), graph.getType(resourceType5)));
        assertEquals(Math.sqrt(70.0 / 6.0), computer.std().get(), delta);

        // if it's not a resource-type
        computer = new Analytics(keyspace, Collections.singleton(graph.getType("thing")));
        assertExceptionThrown(computer::max);
        assertExceptionThrown(computer::min);
        assertExceptionThrown(computer::mean);
        assertExceptionThrown(computer::sum);
        assertExceptionThrown(computer::std);

        // resource-type has no instance
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType3)));
        assertFalse(computer.mean().isPresent());
        assertFalse(computer.max().isPresent());
        assertFalse(computer.min().isPresent());
        assertFalse(computer.sum().isPresent());
        assertFalse(computer.std().isPresent());

        // resource-type has incorrect data type
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(resourceType4)));
        assertExceptionThrown(computer::max);
        assertExceptionThrown(computer::min);
        assertExceptionThrown(computer::mean);
        assertExceptionThrown(computer::sum);
        assertExceptionThrown(computer::std);

        // resource-types have different data types
        computer = new Analytics(keyspace,
                Sets.newHashSet(graph.getType(resourceType1), graph.getType(resourceType2)));
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

    @Test
    public void testMinAndMax() throws Exception {
        // resource-type has no instance
        addOntologyAndEntities();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(graph.getType(resourceType1)));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(graph.getType(thing)),
                Collections.singleton(graph.getType(resourceType2)));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, null,
                Collections.singleton(graph.getType(resourceType1)));
        assertFalse(computer.max().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(graph.getType(thing), graph.getType(anotherThing)),
                Collections.singleton(graph.getType(resourceType2)));
        assertFalse(computer.max().isPresent());


        // add resources, but resources are not connected to any entities
        addResourcesInstances();
        computer = new Analytics(keyspace, null,
                Collections.singleton(graph.getType(resourceType1)));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(graph.getType(thing), graph.getType(anotherThing)),
                Collections.singleton(graph.getType(resourceType2)));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(graph.getType(resourceType1)));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(graph.getType(anotherThing)),
                Collections.singleton(graph.getType(resourceType2)));
        assertFalse(computer.min().isPresent());

        // connect entity and resources
        addResourceRelations();
        computer = new Analytics(keyspace, null,
                Collections.singleton(graph.getType(resourceType1)));
        assertEquals(1.2, computer.min().get().doubleValue(), delta);
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(thing)),
                Collections.singleton(graph.getType(resourceType2)));
        assertEquals(-1L, computer.min().get());
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(graph.getType(resourceType1)));
        assertEquals(1.8, computer.max().get().doubleValue(), delta);
        // TODO: fix this test
        computer = new Analytics(keyspace, Collections.singleton(graph.getType(anotherThing)),
                Collections.singleton(graph.getType(resourceType2)));
        assertEquals(4L, computer.max().get());

    }

    private void addOntologyAndEntities() throws MindmapsValidationException {
        EntityType entityType1 = graph.putEntityType(thing);
        EntityType entityType2 = graph.putEntityType(anotherThing);

        Entity entity1 = graph.putEntity("1", entityType1);
        Entity entity2 = graph.putEntity("2", entityType1);
        Entity entity3 = graph.putEntity("3", entityType1);
        Entity entity4 = graph.putEntity("4", entityType2);

        RoleType relation1 = graph.putRoleType("relation1");
        RoleType relation2 = graph.putRoleType("relation2");
        entityType1.playsRole(relation1).playsRole(relation2);
        entityType2.playsRole(relation1).playsRole(relation2);
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

        List<ResourceType> resourceTypeList = new ArrayList<>();
        resourceTypeList.add(graph.putResourceType(resourceType1, ResourceType.DataType.DOUBLE));
        resourceTypeList.add(graph.putResourceType(resourceType2, ResourceType.DataType.LONG));
        resourceTypeList.add(graph.putResourceType(resourceType3, ResourceType.DataType.LONG));
        resourceTypeList.add(graph.putResourceType(resourceType4, ResourceType.DataType.STRING));
        resourceTypeList.add(graph.putResourceType(resourceType5, ResourceType.DataType.LONG));
        resourceTypeList.add(graph.putResourceType(resourceType6, ResourceType.DataType.DOUBLE));

        RoleType resourceOwner1 = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType1));
        RoleType resourceOwner2 = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType2));
        RoleType resourceOwner3 = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType3));
        RoleType resourceOwner4 = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType4));
        RoleType resourceOwner5 = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType5));
        RoleType resourceOwner6 = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType6));

        RoleType resourceValue1 = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType1));
        RoleType resourceValue2 = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType2));
        RoleType resourceValue3 = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType3));
        RoleType resourceValue4 = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType4));
        RoleType resourceValue5 = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType5));
        RoleType resourceValue6 = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType6));

        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceType1))
                .hasRole(resourceOwner1).hasRole(resourceValue1);
        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceType2))
                .hasRole(resourceOwner2).hasRole(resourceValue2);
        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceType3))
                .hasRole(resourceOwner3).hasRole(resourceValue3);
        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceType4))
                .hasRole(resourceOwner4).hasRole(resourceValue4);
        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceType5))
                .hasRole(resourceOwner5).hasRole(resourceValue5);
        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceType6))
                .hasRole(resourceOwner6).hasRole(resourceValue6);

        entityType1.playsRole(resourceOwner1)
                .playsRole(resourceOwner2)
                .playsRole(resourceOwner3)
                .playsRole(resourceOwner4)
                .playsRole(resourceOwner5)
                .playsRole(resourceOwner6);
        entityType2.playsRole(resourceOwner1)
                .playsRole(resourceOwner2)
                .playsRole(resourceOwner3)
                .playsRole(resourceOwner4)
                .playsRole(resourceOwner5)
                .playsRole(resourceOwner6);

        resourceTypeList.forEach(resourceType -> resourceType
                .playsRole(resourceValue1)
                .playsRole(resourceValue2)
                .playsRole(resourceValue3)
                .playsRole(resourceValue4)
                .playsRole(resourceValue5)
                .playsRole(resourceValue6));


        graph.commit();
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();
    }

    private void addResourcesInstances() throws MindmapsValidationException {
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();

        graph.putResource(1.2, graph.getResourceType(resourceType1));
        graph.putResource(1.5, graph.getResourceType(resourceType1));
        graph.putResource(1.8, graph.getResourceType(resourceType1));

        graph.putResource(4L, graph.getResourceType(resourceType2));
        graph.putResource(-1L, graph.getResourceType(resourceType2));
        graph.putResource(0L, graph.getResourceType(resourceType2));

        graph.putResource(6L, graph.getResourceType(resourceType5));
        graph.putResource(7L, graph.getResourceType(resourceType5));
        graph.putResource(8L, graph.getResourceType(resourceType5));

        graph.putResource(7.2, graph.getResourceType(resourceType6));
        graph.putResource(7.5, graph.getResourceType(resourceType6));
        graph.putResource(7.8, graph.getResourceType(resourceType6));

        graph.putResource("a", graph.getResourceType(resourceType4));
        graph.putResource("b", graph.getResourceType(resourceType4));
        graph.putResource("c", graph.getResourceType(resourceType4));

        graph.commit();
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();
    }

    private void addResourceRelations() throws MindmapsValidationException {
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();

        Entity entity1 = graph.getEntity("1");
        Entity entity2 = graph.getEntity("2");
        Entity entity3 = graph.getEntity("3");
        Entity entity4 = graph.getEntity("4");

        RoleType resourceOwner1 = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType1));
        RoleType resourceOwner2 = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType2));
        RoleType resourceOwner3 = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType3));
        RoleType resourceOwner4 = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType4));
        RoleType resourceOwner5 = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType5));
        RoleType resourceOwner6 = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType6));

        RoleType resourceValue1 = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType1));
        RoleType resourceValue2 = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType2));
        RoleType resourceValue3 = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType3));
        RoleType resourceValue4 = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType4));
        RoleType resourceValue5 = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType5));
        RoleType resourceValue6 = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType6));

        RelationType relationType1 = graph.getRelationType(GraqlType.HAS_RESOURCE.getId(resourceType1));
        graph.addRelation(relationType1)
                .putRolePlayer(resourceOwner1, entity1)
                .putRolePlayer(resourceValue1, graph.putResource(1.2, graph.getResourceType(resourceType1)));
        graph.addRelation(relationType1)
                .putRolePlayer(resourceOwner1, entity1)
                .putRolePlayer(resourceValue1, graph.putResource(1.5, graph.getResourceType(resourceType1)));
        graph.addRelation(relationType1)
                .putRolePlayer(resourceOwner1, entity1)
                .putRolePlayer(resourceValue1, graph.putResource(1.8, graph.getResourceType(resourceType1)));

        RelationType relationType2 = graph.getRelationType(GraqlType.HAS_RESOURCE.getId(resourceType2));
        graph.addRelation(relationType2)
                .putRolePlayer(resourceOwner2, entity1)
                .putRolePlayer(resourceValue2, graph.putResource(4L, graph.getResourceType(resourceType2)));
        graph.addRelation(relationType2)
                .putRolePlayer(resourceOwner2, entity1)
                .putRolePlayer(resourceValue2, graph.putResource(-1L, graph.getResourceType(resourceType2)));
        graph.addRelation(relationType2)
                .putRolePlayer(resourceOwner2, entity4)
                .putRolePlayer(resourceValue2, graph.putResource(0L, graph.getResourceType(resourceType2)));

        RelationType relationType5 = graph.getRelationType(GraqlType.HAS_RESOURCE.getId(resourceType5));
        graph.addRelation(relationType5)
                .putRolePlayer(resourceOwner5, entity1)
                .putRolePlayer(resourceValue5, graph.putResource(7L, graph.getResourceType(resourceType5)));
        graph.addRelation(relationType5)
                .putRolePlayer(resourceOwner5, entity2)
                .putRolePlayer(resourceValue5, graph.putResource(7L, graph.getResourceType(resourceType5)));
        graph.addRelation(relationType5)
                .putRolePlayer(resourceOwner5, entity4)
                .putRolePlayer(resourceValue5, graph.putResource(7L, graph.getResourceType(resourceType5)));

        RelationType relationType6 = graph.getRelationType(GraqlType.HAS_RESOURCE.getId(resourceType6));
        graph.addRelation(relationType6)
                .putRolePlayer(resourceOwner6, entity1)
                .putRolePlayer(resourceValue6, graph.putResource(7.5, graph.getResourceType(resourceType6)));
        graph.addRelation(relationType6)
                .putRolePlayer(resourceOwner6, entity2)
                .putRolePlayer(resourceValue6, graph.putResource(7.5, graph.getResourceType(resourceType6)));
        graph.addRelation(relationType6)
                .putRolePlayer(resourceOwner6, entity4)
                .putRolePlayer(resourceValue6, graph.putResource(7.5, graph.getResourceType(resourceType6)));

        // some resources in, but not connect them to any instances
        graph.putResource(2.8, graph.getResourceType(resourceType1));
        graph.putResource(-5L, graph.getResourceType(resourceType2));
        graph.putResource(10L, graph.getResourceType(resourceType5));
        graph.putResource(0.8, graph.getResourceType(resourceType6));

        graph.commit();
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();
    }
}
