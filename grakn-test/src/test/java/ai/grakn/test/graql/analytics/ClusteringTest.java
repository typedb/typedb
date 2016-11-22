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
import ai.grakn.concept.Entity;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.graql.Graql;
import ai.grakn.graql.internal.analytics.Analytics;
import ai.grakn.graql.internal.analytics.BulkResourceMutate;
import ai.grakn.graql.internal.analytics.GraknVertexProgram;
import ai.grakn.test.AbstractGraphTest;
import ai.grakn.util.Schema;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import ai.grakn.concept.Concept;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.GraknValidationException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.id;
import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class ClusteringTest extends AbstractGraphTest {
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

        logger = (Logger) org.slf4j.LoggerFactory.getLogger(BulkResourceMutate.class);
        logger.setLevel(Level.DEBUG);
    }

    @Ignore //TODO: Stabalise this test. It fails way too often.
    @Test
    public void testConnectedComponent() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        // test on an empty graph
        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());

        Map<String, Long> sizeMap = computer.connectedComponentsSize();
        assertTrue(sizeMap.isEmpty());
        Map<String, Set<String>> memberMap = computer.connectedComponents();
        assertTrue(memberMap.isEmpty());
        Map<String, Long> sizeMapPersist = computer.connectedComponentsAndPersist();
        assertTrue(sizeMapPersist.isEmpty());
        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        assertEquals(0L, computer.count());

        // add something, test again
        addOntologyAndEntities();

        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        sizeMap = computer.connectedComponentsSize();
        assertEquals(1, sizeMap.size());
        assertEquals(7L, sizeMap.values().iterator().next().longValue()); // 4 entities, 3 assertions

        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        memberMap = computer.connectedComponents();
        assertEquals(1, memberMap.size());
        assertEquals(7, memberMap.values().iterator().next().size());
        String clusterLabel = memberMap.keySet().iterator().next();

        long count = computer.count();
        for (int i = 0; i < 3; i++) {
            computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
            sizeMapPersist = computer.connectedComponentsAndPersist();
            graph = Grakn.factory(Grakn.DEFAULT_URI, graph.getKeyspace()).getGraph();
            assertEquals(1, sizeMapPersist.size());
            assertEquals(7L, sizeMapPersist.values().iterator().next().longValue());
            final String finalClusterLabel = clusterLabel;
            instanceIds.forEach(id -> checkConnectedComponent(id, finalClusterLabel));
            assertEquals(count, computer.count());
        }

        // add different resources. This may change existing cluster labels.
        addResourceRelations();

        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        sizeMap = computer.connectedComponentsSize();
        Map<Long, Integer> populationCount00 = new HashMap<>();
        sizeMap.values().forEach(value -> populationCount00.put(value,
                populationCount00.containsKey(value) ? populationCount00.get(value) + 1 : 1));
        assertEquals(5, populationCount00.get(1L).intValue()); // 5 resources are not connected to anything
        assertEquals(1, populationCount00.get(27L).intValue());

        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        memberMap = computer.connectedComponents();
        assertEquals(6, memberMap.size());
        Map<Integer, Integer> populationCount1 = new HashMap<>();
        memberMap.values().forEach(value -> populationCount1.put(value.size(),
                populationCount1.containsKey(value.size()) ? populationCount1.get(value.size()) + 1 : 1));
        assertEquals(5, populationCount1.get(1).intValue());
        assertEquals(1, populationCount1.get(27).intValue());
        clusterLabel = memberMap.entrySet().stream()
                .filter(entry -> entry.getValue().size() != 1)
                .findFirst().get().getKey();

        count = computer.count();
        for (int i = 0; i < 2; i++) {
            computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
            sizeMapPersist = computer.connectedComponentsAndPersist();
            graph = Grakn.factory(Grakn.DEFAULT_URI, graph.getKeyspace()).getGraph();

            Map<Long, Integer> populationCount01 = new HashMap<>();
            sizeMapPersist.values().forEach(value -> populationCount01.put(value,
                    populationCount01.containsKey(value) ? populationCount01.get(value) + 1 : 1));
            assertEquals(6, sizeMapPersist.size());
            assertEquals(5, populationCount01.get(1L).intValue());
            assertEquals(1, populationCount01.get(27L).intValue());
            final String finalClusterLabel = clusterLabel;
            memberMap.values().stream()
                    .filter(set -> set.size() != 1)
                    .flatMap(Collection::stream)
                    .forEach(id -> checkConnectedComponent(id, finalClusterLabel));
            assertEquals(count, computer.count());
        }


        // test on subtypes. This will change existing cluster labels.
        computer = new Analytics(keyspace, Sets.newHashSet(thing, anotherThing, resourceType1, resourceType2,
                resourceType3, resourceType4, resourceType5, resourceType6), new HashSet<>());
        sizeMap = computer.connectedComponentsSize();
        assertEquals(17, sizeMap.size());
        sizeMap.values().forEach(value -> assertEquals(1L, value.longValue()));
        memberMap = computer.connectedComponents();
        assertEquals(17, memberMap.size());
        for (int i = 0; i < 2; i++) {
            computer = new Analytics(keyspace, Sets.newHashSet(thing, anotherThing, resourceType1, resourceType2,
                    resourceType3, resourceType4, resourceType5, resourceType6), new HashSet<>());
            sizeMapPersist = computer.connectedComponentsAndPersist();
            graph = Grakn.factory(Grakn.DEFAULT_URI, graph.getKeyspace()).getGraph();

            assertEquals(17, sizeMapPersist.size());
            memberMap.values().stream()
                    .flatMap(Collection::stream)
                    .forEach(id -> checkConnectedComponent(id, id));
        }
    }

    private void checkConnectedComponent(String id, String expectedClusterLabel) {
        Collection<Resource<?>> resources =
                graph.getInstance(id).resources(graph.getResourceType(Analytics.connectedComponent));
        assertEquals(1, resources.size());
        assertEquals(expectedClusterLabel, resources.iterator().next().getValue());
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

        String relationId12 = relationType.addRelation()
                .putRolePlayer(role1, entity1)
                .putRolePlayer(role2, entity2).getId();
        String relationId23 = relationType.addRelation()
                .putRolePlayer(role1, entity2)
                .putRolePlayer(role2, entity3).getId();
        String relationId24 = relationType.addRelation()
                .putRolePlayer(role1, entity2)
                .putRolePlayer(role2, entity4).getId();
        instanceIds = Lists.newArrayList(entityId1, entityId2, entityId3, entityId4,
                relationId12, relationId23, relationId24);

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

        Entity entity1 = graph.getConcept(entityId1);
        Entity entity2 = graph.getConcept(entityId2);
        Entity entity3 = graph.getConcept(entityId3);
        Entity entity4 = graph.getConcept(entityId4);

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
