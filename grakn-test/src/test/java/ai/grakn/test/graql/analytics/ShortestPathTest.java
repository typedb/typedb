package ai.grakn.test.graql.analytics;

import ai.grakn.Grakn;
import ai.grakn.concept.*;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.internal.analytics.Analytics;
import ai.grakn.graql.internal.analytics.GraknVertexProgram;
import ai.grakn.test.AbstractGraphTest;
import ai.grakn.util.Schema;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

public class ShortestPathTest extends AbstractGraphTest {
    private static final String thing = "thing";
    private static final String anotherThing = "anotherThing";
    private static final String related = "related";

    private static final String resourceType1 = "resourceType1";
    private static final String resourceType2 = "resourceType2";
    private static final String resourceType3 = "resourceType3";
    private static final String resourceType4 = "resourceType4";
    private static final String resourceType5 = "resourceType5";
    private static final String resourceType6 = "resourceType6";
    private static final String resourceType7 = "resourceType7";

    private String entityId1;
    private String entityId2;
    private String entityId3;
    private String entityId4;
    private String entityId5;
    private String relationId12;
    private String relationId13;
    private String relationId24;
    private String relationId34;
    private List<String> instanceIds;

    String keyspace;
    Analytics computer;

    @Before
    public void setUp() {
        // TODO: Fix tests in orientdb
        assumeFalse(usingOrientDB());

        keyspace = graph.getKeyspace();

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(GraknVertexProgram.class);
        logger.setLevel(Level.DEBUG);
    }

    @Test(expected = IllegalStateException.class)
    public void testShortestPathExceptionIdNotFound() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        // test on an empty graph
        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        computer.shortestPath(entityId1, entityId2);
    }

    @Test(expected = IllegalStateException.class)
    public void testShortestPathExceptionIdNotFoundSubgraph() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        addOntologyAndEntities();
        computer = new Analytics(keyspace, Sets.newHashSet(thing, related), new HashSet<>());
        computer.shortestPath(entityId1, entityId4);
    }

    @Test(expected = RuntimeException.class)
    public void testShortestPathExceptionPathNotFound() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        addOntologyAndEntities();
        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        computer.shortestPath(entityId1, entityId5);
    }

    @Test
    public void testShortestPath() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        List<String> correctPath;
        List<String> result;
        addOntologyAndEntities();

        // directly connected vertices
        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        result = computer.shortestPath(entityId1, relationId12);
        correctPath = Lists.newArrayList(entityId1, relationId12);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }
        result = computer.shortestPath(relationId12, entityId1);
        Collections.reverse(result);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }

        // entities connected by a relation
        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        result = computer.shortestPath(entityId1, entityId2);
        correctPath = Lists.newArrayList(entityId1, relationId12, entityId2);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }
        result = computer.shortestPath(entityId2, entityId1);
        Collections.reverse(result);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }

        // only one path exists with given subtypes
        computer = new Analytics(keyspace, Sets.newHashSet(thing, related), new HashSet<>());
        result = computer.shortestPath(entityId2, entityId3);
        correctPath = Lists.newArrayList(entityId2, relationId12, entityId1, relationId13, entityId3);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }
        result = computer.shortestPath(entityId3, entityId2);
        Collections.reverse(result);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }

        computer = new Analytics(keyspace, Sets.newHashSet(thing, related), new HashSet<>());
        result = computer.shortestPath(entityId1, entityId2);
        correctPath = Lists.newArrayList(entityId1, relationId12, entityId2);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }
        result = computer.shortestPath(entityId2, entityId1);
        Collections.reverse(result);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }

        // add different resources.
        addResourceRelations();

        computer = new Analytics(keyspace, Sets.newHashSet(thing, anotherThing, resourceType1, resourceType2,
                resourceType3, resourceType4, resourceType5, resourceType6), new HashSet<>());
    }

    private void addOntologyAndEntities() throws GraknValidationException {
        EntityType entityType1 = graph.putEntityType(thing);
        EntityType entityType2 = graph.putEntityType(anotherThing);

        Entity entity1 = entityType1.addEntity();
        Entity entity2 = entityType1.addEntity();
        Entity entity3 = entityType1.addEntity();
        Entity entity4 = entityType2.addEntity();
        Entity entity5 = entityType1.addEntity();

        entityId1 = entity1.getId();
        entityId2 = entity2.getId();
        entityId3 = entity3.getId();
        entityId4 = entity4.getId();
        entityId5 = entity5.getId();

        RoleType role1 = graph.putRoleType("role1");
        RoleType role2 = graph.putRoleType("role2");
        entityType1.playsRole(role1).playsRole(role2);
        entityType2.playsRole(role1).playsRole(role2);
        RelationType relationType = graph.putRelationType(related).hasRole(role1).hasRole(role2);

        relationId12 = relationType.addRelation()
                .putRolePlayer(role1, entity1)
                .putRolePlayer(role2, entity2).getId();
        relationId13 = relationType.addRelation()
                .putRolePlayer(role1, entity1)
                .putRolePlayer(role2, entity3).getId();
        relationId24 = relationType.addRelation()
                .putRolePlayer(role1, entity2)
                .putRolePlayer(role2, entity4).getId();
        relationId34 = relationType.addRelation()
                .putRolePlayer(role1, entity3)
                .putRolePlayer(role2, entity4).getId();
        instanceIds = Lists.newArrayList(entityId1, entityId2, entityId3, entityId4,
                relationId12, relationId13, relationId24, relationId34);

        List<ResourceType> resourceTypeList = new ArrayList<>();
        resourceTypeList.add(graph.putResourceType(resourceType1, ResourceType.DataType.DOUBLE));
        resourceTypeList.add(graph.putResourceType(resourceType2, ResourceType.DataType.LONG));
        resourceTypeList.add(graph.putResourceType(resourceType3, ResourceType.DataType.LONG));
        resourceTypeList.add(graph.putResourceType(resourceType4, ResourceType.DataType.STRING));
        resourceTypeList.add(graph.putResourceType(resourceType5, ResourceType.DataType.LONG));
        resourceTypeList.add(graph.putResourceType(resourceType6, ResourceType.DataType.DOUBLE));
        resourceTypeList.add(graph.putResourceType(resourceType7, ResourceType.DataType.DOUBLE));

        RoleType resourceOwner1 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceType1));
        RoleType resourceOwner2 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceType2));
        RoleType resourceOwner3 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceType3));
        RoleType resourceOwner4 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceType4));
        RoleType resourceOwner5 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceType5));
        RoleType resourceOwner6 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceType6));
        RoleType resourceOwner7 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceType7));

        RoleType resourceValue1 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceType1));
        RoleType resourceValue2 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceType2));
        RoleType resourceValue3 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceType3));
        RoleType resourceValue4 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceType4));
        RoleType resourceValue5 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceType5));
        RoleType resourceValue6 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceType6));
        RoleType resourceValue7 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceType7));

        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getId(resourceType1))
                .hasRole(resourceOwner1).hasRole(resourceValue1);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getId(resourceType2))
                .hasRole(resourceOwner2).hasRole(resourceValue2);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getId(resourceType3))
                .hasRole(resourceOwner3).hasRole(resourceValue3);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getId(resourceType4))
                .hasRole(resourceOwner4).hasRole(resourceValue4);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getId(resourceType5))
                .hasRole(resourceOwner5).hasRole(resourceValue5);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getId(resourceType6))
                .hasRole(resourceOwner6).hasRole(resourceValue6);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getId(resourceType7))
                .hasRole(resourceOwner7).hasRole(resourceValue7);

        entityType1.playsRole(resourceOwner1)
                .playsRole(resourceOwner2)
                .playsRole(resourceOwner3)
                .playsRole(resourceOwner4)
                .playsRole(resourceOwner5)
                .playsRole(resourceOwner6)
                .playsRole(resourceOwner7);
        entityType2.playsRole(resourceOwner1)
                .playsRole(resourceOwner2)
                .playsRole(resourceOwner3)
                .playsRole(resourceOwner4)
                .playsRole(resourceOwner5)
                .playsRole(resourceOwner6)
                .playsRole(resourceOwner7);

        resourceTypeList.forEach(resourceType -> resourceType
                .playsRole(resourceValue1)
                .playsRole(resourceValue2)
                .playsRole(resourceValue3)
                .playsRole(resourceValue4)
                .playsRole(resourceValue5)
                .playsRole(resourceValue6)
                .playsRole(resourceValue7));

        graph.commit();
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
    }

    private void addResourceRelations() throws GraknValidationException {
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();

        Entity entity1 = graph.getEntity(entityId1);
        Entity entity2 = graph.getEntity(entityId2);
        Entity entity3 = graph.getEntity(entityId3);
        Entity entity4 = graph.getEntity(entityId4);

        RoleType resourceOwner1 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceType1));
        RoleType resourceOwner2 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceType2));
        RoleType resourceOwner3 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceType3));
        RoleType resourceOwner4 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceType4));
        RoleType resourceOwner5 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceType5));
        RoleType resourceOwner6 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceType6));

        RoleType resourceValue1 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceType1));
        RoleType resourceValue2 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceType2));
        RoleType resourceValue3 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceType3));
        RoleType resourceValue4 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceType4));
        RoleType resourceValue5 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceType5));
        RoleType resourceValue6 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceType6));

        RelationType relationType1 = graph.getRelationType(Schema.Resource.HAS_RESOURCE.getId(resourceType1));
        relationType1.addRelation()
                .putRolePlayer(resourceOwner1, entity1)
                .putRolePlayer(resourceValue1, graph.getResourceType(resourceType1).putResource(1.2));
        relationType1.addRelation()
                .putRolePlayer(resourceOwner1, entity1)
                .putRolePlayer(resourceValue1, graph.getResourceType(resourceType1).putResource(1.5));
        relationType1.addRelation()
                .putRolePlayer(resourceOwner1, entity3)
                .putRolePlayer(resourceValue1, graph.getResourceType(resourceType1).putResource(1.8));

        RelationType relationType2 = graph.getRelationType(Schema.Resource.HAS_RESOURCE.getId(resourceType2));
        relationType2.addRelation()
                .putRolePlayer(resourceOwner2, entity1)
                .putRolePlayer(resourceValue2, graph.getResourceType(resourceType2).putResource(4L));
        relationType2.addRelation()
                .putRolePlayer(resourceOwner2, entity1)
                .putRolePlayer(resourceValue2, graph.getResourceType(resourceType2).putResource(-1L));
        relationType2.addRelation()
                .putRolePlayer(resourceOwner2, entity4)
                .putRolePlayer(resourceValue2, graph.getResourceType(resourceType2).putResource(0L));

        RelationType relationType5 = graph.getRelationType(Schema.Resource.HAS_RESOURCE.getId(resourceType5));
        relationType5.addRelation()
                .putRolePlayer(resourceOwner5, entity1)
                .putRolePlayer(resourceValue5, graph.getResourceType(resourceType5).putResource(-7L));
        relationType5.addRelation()
                .putRolePlayer(resourceOwner5, entity2)
                .putRolePlayer(resourceValue5, graph.getResourceType(resourceType5).putResource(-7L));
        relationType5.addRelation()
                .putRolePlayer(resourceOwner5, entity4)
                .putRolePlayer(resourceValue5, graph.getResourceType(resourceType5).putResource(-7L));

        RelationType relationType6 = graph.getRelationType(Schema.Resource.HAS_RESOURCE.getId(resourceType6));
        relationType6.addRelation()
                .putRolePlayer(resourceOwner6, entity1)
                .putRolePlayer(resourceValue6, graph.getResourceType(resourceType6).putResource(7.5));
        relationType6.addRelation()
                .putRolePlayer(resourceOwner6, entity2)
                .putRolePlayer(resourceValue6, graph.getResourceType(resourceType6).putResource(7.5));
        relationType6.addRelation()
                .putRolePlayer(resourceOwner6, entity4)
                .putRolePlayer(resourceValue6, graph.getResourceType(resourceType6).putResource(7.5));

        // some resources in, but not connect them to any instances
        graph.getResourceType(resourceType1).putResource(2.8);
        graph.getResourceType(resourceType2).putResource(-5L);
        graph.getResourceType(resourceType3).putResource(100L);
        graph.getResourceType(resourceType5).putResource(10L);
        graph.getResourceType(resourceType6).putResource(0.8);

        graph.commit();
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
    }
}
