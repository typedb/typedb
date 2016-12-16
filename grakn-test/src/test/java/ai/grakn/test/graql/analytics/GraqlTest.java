/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.graql.analytics;

import ai.grakn.Grakn;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.analytics.ClusterQuery;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.internal.analytics.Analytics;
import ai.grakn.graql.internal.analytics.BulkResourceMutate;
import ai.grakn.graql.internal.analytics.GraknVertexProgram;
import ai.grakn.test.AbstractGraphTest;
import ai.grakn.util.Schema;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
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
    private String relationId12;
    private String relationId24;

    private String keyspace;

    @Before
    public void setUp() {
        // TODO: Make orientdb support analytics
        assumeFalse(usingOrientDB());
        qb = graph.graql();
        keyspace = graph.getKeyspace();

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(GraknVertexProgram.class);
        logger.setLevel(Level.DEBUG);

        logger = (Logger) org.slf4j.LoggerFactory.getLogger(BulkResourceMutate.class);
        logger.setLevel(Level.DEBUG);
    }

    @Test
    public void testGraqlCount() throws GraknValidationException, InterruptedException, ExecutionException {
        addOntologyAndEntities();
        assertEquals(6L, ((Long) qb.parse("compute count;").execute()).longValue());
        assertEquals(3L, ((Long) qb.parse("compute count in thing, thing;").execute()).longValue());
    }

    @Test
    public void testDegrees() throws Exception {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        addOntologyAndEntities();
        Map<Long, Set<String>> degrees = ((Map<Long, Set<String>>) qb.parse("compute degrees;").execute());

        Map<String, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entityId1, 1L);
        correctDegrees.put(entityId2, 2L);
        correctDegrees.put(entityId3, 0L);
        correctDegrees.put(entityId4, 1L);
        correctDegrees.put(relationId12, 2L);
        correctDegrees.put(relationId24, 2L);

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

        addOntologyAndEntities();
        qb.parse("compute degrees; persist;").execute();

        Map<String, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entityId1, 1L);
        correctDegrees.put(entityId2, 2L);
        correctDegrees.put(entityId3, 0L);
        correctDegrees.put(entityId4, 1L);
        correctDegrees.put(relationId12, 2L);
        correctDegrees.put(relationId24, 2L);

        graph = Grakn.factory(Grakn.DEFAULT_URI, graph.getKeyspace()).getGraph();

        correctDegrees.entrySet().forEach(entry -> {
            Collection<Resource<?>> resources =
                    graph.getConcept(entry.getKey()).asInstance().resources(graph.getResourceType(Analytics.degree));
            assertEquals(1, resources.size());
            assertEquals(entry.getValue(),resources.iterator().next().getValue());
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidIdWithAnalytics() {
        qb.parse("compute sum of thing;").execute();
    }

    @Test
    public void testStatisticsMethods() throws GraknValidationException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        String resourceTypeId = "my-resource";

        RoleType resourceOwner = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceTypeId));
        RoleType resourceValue = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceTypeId));
        RelationType relationType = graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceTypeId))
                .hasRole(resourceOwner)
                .hasRole(resourceValue);

        ResourceType<Long> resource = graph.putResourceType(resourceTypeId, ResourceType.DataType.LONG)
                .playsRole(resourceValue);
        EntityType thing = graph.putEntityType("thing").playsRole(resourceOwner);
        Entity theResourceOwner = thing.addEntity();

        relationType.addRelation()
                .putRolePlayer(resourceOwner, theResourceOwner)
                .putRolePlayer(resourceValue, resource.putResource(1L));
        relationType.addRelation()
                .putRolePlayer(resourceOwner, theResourceOwner)
                .putRolePlayer(resourceValue, resource.putResource(2L));
        relationType.addRelation()
                .putRolePlayer(resourceOwner, theResourceOwner)
                .putRolePlayer(resourceValue, resource.putResource(3L));

        graph.commit();

        // use graql to compute various statistics
        Optional<Number> result = (Optional<Number>) qb.parse("compute sum of my-resource;").execute();
        assertEquals(6L, (long) result.orElse(0L));
        result = (Optional<Number>) qb.parse("compute min of my-resource;").execute();
        assertEquals(1L, (long) result.orElse(0L));
        result = (Optional<Number>) qb.parse("compute max of my-resource;").execute();
        assertEquals(3L, (long) result.orElse(0L));
        result = (Optional<Number>) qb.parse("compute mean of my-resource;").execute();
        assertEquals(2.0, (double) result.orElse(0L), 0.1);
        result = (Optional<Number>) qb.parse("compute median of my-resource;").execute();
        assertEquals(2L, (long) result.orElse(0L));
    }

    @Test
    public void testConnectedComponents() throws GraknValidationException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        Map<String, Long> sizeMap =
                qb.<ClusterQuery<Map<String, Long>>>parse("compute cluster;").execute();
        assertTrue(sizeMap.isEmpty());
        Map<String, Set<String>> memberMap =
                qb.<ClusterQuery<Map<String, Set<String>>>>parse("compute cluster; members;").execute();
        assertTrue(memberMap.isEmpty());
        Map<String, Long> sizeMapPersist =
                qb.<ClusterQuery<Map<String, Long>>>parse("compute cluster; persist;").execute();
        assertTrue(sizeMapPersist.isEmpty());
    }

    @Test
    public void testPath() throws GraknValidationException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        addOntologyAndEntities();

        PathQuery query = qb.parse("compute path from '" + entityId1 + "' to '" + entityId2 + "';");

        List<String> result = query.execute().get().stream().map(Concept::getId).collect(Collectors.toList());

        List<String> expected = Lists.newArrayList(entityId1, relationId12, entityId2);

        assertEquals(expected, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonResourceTypeAsSubgraphForAnalytics() throws GraknValidationException {
        graph.putEntityType(thing);
        graph.commit();

        qb.parse("compute sum in thing;").execute();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testErrorWhenNoSubgrapForAnalytics() throws GraknValidationException {
        qb.parse("compute sum;").execute();
        qb.parse("compute min;").execute();
        qb.parse("compute max;").execute();
        qb.parse("compute mean;").execute();
        qb.parse("compute std;").execute();
    }

    @Test
    public void testAnalyticsDoesNotCommitByMistake() throws GraknValidationException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        graph.putResourceType("number", ResourceType.DataType.LONG);
        graph.commit();

        Set<String> analyticsCommands = new HashSet<>(Arrays.asList(
                "compute count;",
                "compute degrees;",
                "compute mean of number;"));

        analyticsCommands.forEach(command -> {
            // insert a node but do not commit it
            qb.parse("insert thing sub entity;").execute();
            // use analytics
            qb.parse(command).execute();
            // see if the node was commited
            graph.rollback();
            assertNull(graph.getEntityType("thing"));
        });
    }

    private void addOntologyAndEntities() throws GraknValidationException {
        EntityType entityType1 = graph.putEntityType(thing);
        EntityType entityType2 = graph.putEntityType(anotherThing);

        Entity entity1 = entityType1.addEntity();
        Entity entity2 = entityType1.addEntity();
        Entity entity3 = entityType1.addEntity();
        Entity entity4 = entityType2.addEntity();

        entityId1 = entity1.getId();
        entityId2 = entity2.getId();
        entityId3 = entity3.getId();
        entityId4 = entity4.getId();

        RoleType role1 = graph.putRoleType("role1");
        RoleType role2 = graph.putRoleType("role2");
        entityType1.playsRole(role1).playsRole(role2);
        entityType2.playsRole(role1).playsRole(role2);
        RelationType relationType = graph.putRelationType(related).hasRole(role1).hasRole(role2);

        relationId12 = relationType.addRelation()
                .putRolePlayer(role1, entity1)
                .putRolePlayer(role2, entity2).getId();
        relationId24 = relationType.addRelation()
                .putRolePlayer(role1, entity2)
                .putRolePlayer(role2, entity4).getId();

        graph.commit();
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
    }
}
