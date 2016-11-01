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

package io.mindmaps.test.graql.analytics;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.Lists;
import io.mindmaps.Mindmaps;
import io.mindmaps.concept.*;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.graql.ComputeQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.internal.analytics.Analytics;
import io.mindmaps.graql.internal.analytics.MindmapsVertexProgram;
import io.mindmaps.graql.internal.util.GraqlType;
import io.mindmaps.test.AbstractGraphTest;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static io.mindmaps.graql.Graql.*;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public class GraqlTest extends AbstractGraphTest {

    private QueryBuilder qb;

    private static final String thing = "thing";
    private static final String anotherThing = "anotherThing";
    private static final String related = "related";

    private String entityId1;
    private String entityId2;
    private String entityId3;
    private String entityId4;
    private List<String> instanceIds;

    String keyspace;

    @Before
    public void setUp() {
        // TODO: Make orientdb support analytics
        assumeFalse(usingOrientDB());
        qb = withGraph(graph);
        keyspace = graph.getKeyspace();

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(MindmapsVertexProgram.class);
        logger.setLevel(Level.DEBUG);
    }

    @Test
    public void testGraqlCount() throws MindmapsValidationException, InterruptedException, ExecutionException {

        // assert the graph is empty
        Analytics computer = new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<>());
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

        long computeCount = ((Long) ((ComputeQuery) qb.parse("compute count;")).execute());

        assertEquals(graqlCount, computeCount);
        assertEquals(3L, computeCount);

        computeCount = ((Long) ((ComputeQuery) qb.parse("compute count;")).execute());

        assertEquals(graqlCount, computeCount);
        assertEquals(3L, computeCount);
    }

    @Test
    public void testGraqlCountSubgraph() throws Exception {
        // create 3 instances
        EntityType thing = graph.putEntityType("thing");
        EntityType anotherThing = graph.putEntityType("another");
        graph.putEntity("1", thing);
        graph.putEntity("2", thing);
        graph.putEntity("3", anotherThing);
        graph.commit();

        long computeCount = ((Long) ((ComputeQuery) qb.parse("compute count in thing, thing;")).execute());
        assertEquals(2, computeCount);
    }


    @Test
    public void testDegrees() throws Exception {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create 3 instances
        EntityType thing = graph.putEntityType("thing");
        String entity1 = graph.addEntity(thing).getId();
        String entity2 = graph.addEntity(thing).getId();
        String entity3 = graph.addEntity(thing).getId();
        String entity4 = graph.addEntity(thing).getId();

        RoleType role1 = graph.putRoleType("role1");
        RoleType role2 = graph.putRoleType("role2");
        thing.playsRole(role1).playsRole(role2);
        RelationType related = graph.putRelationType("related").hasRole(role1).hasRole(role2);

        // relate them
        String id1 = graph.addRelation(related)
                .putRolePlayer(role1, graph.getInstance(entity1))
                .putRolePlayer(role2, graph.getInstance(entity2))
                .getId();

        String id2 = graph.addRelation(related)
                .putRolePlayer(role1, graph.getInstance(entity2))
                .putRolePlayer(role2, graph.getInstance(entity3))
                .getId();

        String id3 = graph.addRelation(related)
                .putRolePlayer(role1, graph.getInstance(entity2))
                .putRolePlayer(role2, graph.getInstance(entity4))
                .getId();

        graph.commit();

        // compute degrees
        Map<Long, Set<String>> degrees = ((Map) qb.parse("compute degrees;").execute());

        // assert degrees are correct
        Map<String, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(entity4, 1l);
        correctDegrees.put(id1, 2l);
        correctDegrees.put(id2, 2l);
        correctDegrees.put(id3, 2l);

        assertTrue(!degrees.isEmpty());
        degrees.entrySet().forEach(entry -> entry.getValue().forEach(
                id -> {
                    assertTrue(correctDegrees.containsKey(id));
                    assertEquals(correctDegrees.get(id), entry.getKey());
                }
        ));
    }

    @Test
    public void testDegreesAndPersist() throws Exception {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create 3 instances
        EntityType thing = graph.putEntityType("thing");
        Entity entity1 = graph.putEntity("1", thing);
        Entity entity2 = graph.putEntity("2", thing);
        Entity entity3 = graph.putEntity("3", thing);
        Entity entity4 = graph.putEntity("4", thing);

        RoleType relation1 = graph.putRoleType("relation1");
        RoleType relation2 = graph.putRoleType("relation2");
        thing.playsRole(relation1).playsRole(relation2);
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

        graph.commit();

        // compute degrees
        ((ComputeQuery) qb.parse("compute degreesAndPersist;")).execute();

        // assert persisted degrees are correct
//        MindmapsGraph graph = MindmapsGraphFactoryImpl.getGraph(keyspace);
        entity1 = graph.getEntity("1");
        entity2 = graph.getEntity("2");
        entity3 = graph.getEntity("3");
        entity4 = graph.getEntity("4");

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

        // compute degrees again
        ((ComputeQuery) qb.parse("compute degreesAndPersist;")).execute();

        // assert persisted degrees are correct
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, graph.getKeyspace()).getGraph();
        entity1 = graph.getEntity("1");
        entity2 = graph.getEntity("2");
        entity3 = graph.getEntity("3");
        entity4 = graph.getEntity("4");

        correctDegrees = new HashMap<>();
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

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidIdWithAnalytics() {
        ((ComputeQuery) qb.parse("compute sum of thing;")).execute();
    }

    @Test
    public void testStatisticsMethods() throws MindmapsValidationException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        String resourceTypeId = "resource";

        RoleType resourceOwner = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceTypeId));
        RoleType resourceValue = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceTypeId));
        RelationType relationType = graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceTypeId))
                .hasRole(resourceOwner)
                .hasRole(resourceValue);

        ResourceType<Long> resource = graph.putResourceType(resourceTypeId, ResourceType.DataType.LONG)
                .playsRole(resourceValue);
        EntityType thing = graph.putEntityType("thing").playsRole(resourceOwner);
        Entity theResourceOwner = graph.addEntity(thing);

        graph.addRelation(relationType)
                .putRolePlayer(resourceOwner, theResourceOwner)
                .putRolePlayer(resourceValue, graph.putResource(1L, resource));
        graph.addRelation(relationType)
                .putRolePlayer(resourceOwner, theResourceOwner)
                .putRolePlayer(resourceValue, graph.putResource(2L, resource));
        graph.addRelation(relationType)
                .putRolePlayer(resourceOwner, theResourceOwner)
                .putRolePlayer(resourceValue, graph.putResource(3L, resource));

        graph.commit();

        // use graql to compute various statistics
        Optional<Number> result = (Optional<Number>) ((ComputeQuery) qb.parse("compute sum of resource;")).execute();
        assertEquals(6L, (long) result.orElse(0L));
        result = (Optional<Number>) ((ComputeQuery) qb.parse("compute min of resource;")).execute();
        assertEquals(1L, (long) result.orElse(0L));
        result = (Optional<Number>) ((ComputeQuery) qb.parse("compute max of resource;")).execute();
        assertEquals(3L, (long) result.orElse(0L));
        result = (Optional<Number>) ((ComputeQuery) qb.parse("compute mean of resource;")).execute();
        assertEquals(2.0, (double) result.orElse(0L), 0.1);
        result = (Optional<Number>) ((ComputeQuery) qb.parse("compute median of resource;")).execute();
        assertEquals(2L, (long) result.orElse(0L));
    }

    @Test
    public void testConnectedComponents() throws MindmapsValidationException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        Map<String, Long> sizeMap =
                (Map<String, Long>) qb.parse("compute connectedComponentsSize;").execute();
        assertTrue(sizeMap.isEmpty());
        Map<String, Set<String>> memberMap =
                (Map<String, Set<String>>) qb.parse("compute connectedComponents;").execute();
        assertTrue(memberMap.isEmpty());
        Map<String, Long> sizeMapPersist =
                (Map<String, Long>) qb.parse("compute connectedComponentsAndPersist;").execute();
        assertTrue(sizeMapPersist.isEmpty());
    }

    @Test(expected = IllegalStateException.class)
    public void testNonResourceTypeAsSubgraphForAnalytics() throws MindmapsValidationException {
        EntityType thing = graph.putEntityType("thing");
        graph.commit();

        ((ComputeQuery) qb.parse("compute sum in thing;")).execute();
    }

    @Test(expected = IllegalStateException.class)
    public void testErrorWhenNoSubgrapForAnalytics() throws MindmapsValidationException {
        ((ComputeQuery) qb.parse("compute sum;")).execute();
        ((ComputeQuery) qb.parse("compute min;")).execute();
        ((ComputeQuery) qb.parse("compute max;")).execute();
        ((ComputeQuery) qb.parse("compute mean;")).execute();
    }

    @Test
    public void testAnalyticsDoesNotCommitByMistake() throws MindmapsValidationException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        graph.putResourceType("number", ResourceType.DataType.LONG);
        graph.commit();

        Set<String> analyticsCommands = new HashSet<String>(Arrays.asList(
                "compute count;",
                "compute degrees;",
                "compute mean of number;"));

        analyticsCommands.forEach(command -> {
            // insert a node but do not commit it
            qb.parse("insert thing isa entity-type;").execute();
            // use analytics
            qb.parse(command).execute();
            // see if the node was commited
            graph.rollback();
            assertNull(graph.getEntityType("thing"));
        });
    }

    private void addOntologyAndEntities() throws MindmapsValidationException {
        EntityType entityType1 = graph.putEntityType(thing);
        EntityType entityType2 = graph.putEntityType(anotherThing);

        Entity entity1 = graph.addEntity(entityType1);
        Entity entity2 = graph.addEntity(entityType1);
        Entity entity3 = graph.addEntity(entityType1);
        Entity entity4 = graph.addEntity(entityType2);
        entityId1 = entity1.getId();
        entityId2 = entity2.getId();
        entityId3 = entity3.getId();
        entityId4 = entity4.getId();

        RoleType role1 = graph.putRoleType("role1");
        RoleType role2 = graph.putRoleType("role2");
        entityType1.playsRole(role1).playsRole(role2);
        entityType2.playsRole(role1).playsRole(role2);
        RelationType relationType = graph.putRelationType(related).hasRole(role1).hasRole(role2);

        String relationId12 = graph.addRelation(relationType)
                .putRolePlayer(role1, entity1)
                .putRolePlayer(role2, entity2).getId();
        String relationId23 = graph.addRelation(relationType)
                .putRolePlayer(role1, entity2)
                .putRolePlayer(role2, entity3).getId();
        String relationId24 = graph.addRelation(relationType)
                .putRolePlayer(role1, entity2)
                .putRolePlayer(role2, entity4).getId();
        instanceIds = Lists.newArrayList(entityId1, entityId2, entityId3, entityId4,
                relationId12, relationId23, relationId24);
    }
}
