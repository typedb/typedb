package ai.grakn.test.graql.analytics;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.*;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.internal.analytics.Analytics;
import ai.grakn.graql.internal.analytics.GraknVertexProgram;
import ai.grakn.test.AbstractGraphTest;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

public class ShortestPathTest extends AbstractGraphTest {
    private static final String thing = "thing";
    private static final String anotherThing = "anotherThing";
    private static final String related = "related";
    private static final String veryRelated = "veryRelated";

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
    private String relationId1A12;

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
        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();

        List<String> correctPath;
        List<String> result;
        addOntologyAndEntities();

        // directly connected vertices
        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        result = computer.shortestPath(entityId1, relationId12)
                .stream().map(Concept::getId).collect(Collectors.toList());
        correctPath = Lists.newArrayList(entityId1, relationId12);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }
        result = computer.shortestPath(relationId12, entityId1)
                .stream().map(Concept::getId).collect(Collectors.toList());
        Collections.reverse(result);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }

        // entities connected by a relation
        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        result = computer.shortestPath(entityId1, entityId2)
                .stream().map(Concept::getId).collect(Collectors.toList());
        correctPath = Lists.newArrayList(entityId1, relationId12, entityId2);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }
        result = computer.shortestPath(entityId2, entityId1)
                .stream().map(Concept::getId).collect(Collectors.toList());
        Collections.reverse(result);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }

        // only one path exists with given subtypes
        computer = new Analytics(keyspace, Sets.newHashSet(thing, related), new HashSet<>());
        result = computer.shortestPath(entityId2, entityId3)
                .stream().map(Concept::getId).collect(Collectors.toList());
        correctPath = Lists.newArrayList(entityId2, relationId12, entityId1, relationId13, entityId3);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }
        result = computer.shortestPath(entityId3, entityId2)
                .stream().map(Concept::getId).collect(Collectors.toList());
        Collections.reverse(result);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }

        computer = new Analytics(keyspace, Sets.newHashSet(thing, related), new HashSet<>());
        result = computer.shortestPath(entityId1, entityId2)
                .stream().map(Concept::getId).collect(Collectors.toList());
        correctPath = Lists.newArrayList(entityId1, relationId12, entityId2);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }
        result = computer.shortestPath(entityId2, entityId1)
                .stream().map(Concept::getId).collect(Collectors.toList());
        Collections.reverse(result);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }
    }

    @Test
    public void testShortestPathCastingWithThreeMessages() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        List<String> correctPath;
        List<String> result;
        addOntologyAndEntities2();

        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        result = computer.shortestPath(entityId2, entityId3)
                .stream().map(Concept::getId).collect(Collectors.toList());
        correctPath = Lists.newArrayList(entityId2, relationId12, entityId1, relationId13, entityId3);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }
        result = computer.shortestPath(entityId3, entityId2)
                .stream().map(Concept::getId).collect(Collectors.toList());
        Collections.reverse(result);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }

        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        result = computer.shortestPath(relationId1A12, entityId3)
                .stream().map(Concept::getId).collect(Collectors.toList());
        correctPath = Lists.newArrayList(relationId1A12, entityId1, relationId13, entityId3);
        System.out.println("result = " + result);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }
        result = computer.shortestPath(entityId3, relationId1A12)
                .stream().map(Concept::getId).collect(Collectors.toList());
        Collections.reverse(result);
        assertEquals(correctPath.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(correctPath.get(i), result.get(i));
        }
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

        graph.commit();
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
    }

    private void addOntologyAndEntities2() throws GraknValidationException {
        EntityType entityType = graph.putEntityType(thing);

        Entity entity1 = entityType.addEntity();
        Entity entity2 = entityType.addEntity();
        Entity entity3 = entityType.addEntity();

        entityId1 = entity1.getId();
        entityId2 = entity2.getId();
        entityId3 = entity3.getId();

        RoleType role1 = graph.putRoleType("role1");
        RoleType role2 = graph.putRoleType("role2");
        entityType.playsRole(role1).playsRole(role2);
        RelationType relationType = graph.putRelationType(related).hasRole(role1).hasRole(role2);

        RoleType role3 = graph.putRoleType("role3");
        RoleType role4 = graph.putRoleType("role4");
        entityType.playsRole(role3).playsRole(role4);
        relationType.playsRole(role3).playsRole(role4);
        RelationType relationType2 = graph.putRelationType(veryRelated).hasRole(role3).hasRole(role4);

        relationId12 = relationType.addRelation()
                .putRolePlayer(role1, entity1)
                .putRolePlayer(role2, entity2).getId();
        relationId13 = relationType.addRelation()
                .putRolePlayer(role1, entity1)
                .putRolePlayer(role2, entity3).getId();

        relationId1A12 = relationType2.addRelation()
                .putRolePlayer(role3, entity1)
                .putRolePlayer(role4, graph.getConcept(relationId12)).getId();

        graph.commit();
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
    }
}
