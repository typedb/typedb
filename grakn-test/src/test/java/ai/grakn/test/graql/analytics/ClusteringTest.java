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

import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graph.internal.computer.GraknSparkComputer;
import ai.grakn.graql.Graql;
import ai.grakn.test.EngineContext;
import ai.grakn.util.Schema;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.test.GraknTestEnv.usingOrientDB;
import static ai.grakn.test.GraknTestEnv.usingTinker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class ClusteringTest {
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

    private ConceptId entityId1;
    private ConceptId entityId2;
    private ConceptId entityId3;
    private ConceptId entityId4;
    private List<ConceptId> instanceIds;

    @ClassRule
    public static final EngineContext context = EngineContext.startInMemoryServer();
    private GraknSession factory;

    @Before
    public void setUp() {
        // TODO: Fix tests in orientdb
        assumeFalse(usingOrientDB());

        factory = context.factoryWithNewKeyspace();
    }

    @Test
    public void testConnectedComponentOnEmptyGraph() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            // test on an empty rule.graph()
            Map<String, Long> sizeMap = Graql.compute().withGraph(graph).cluster().execute();
            assertTrue(sizeMap.isEmpty());
            Map<String, Set<String>> memberMap = graph.graql().compute().cluster().members().execute();
            assertTrue(memberMap.isEmpty());

            assertEquals(0L, graph.graql().compute().count().execute().longValue());
        }
    }

    @Test
    public void testConnectedComponentSize() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        Map<String, Long> sizeMap;
        Map<String, Set<String>> memberMap;
        Map<String, Long> sizeMapPersist;
        Map<String, Set<String>> memberMapPersist;

        addOntologyAndEntities();

        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            sizeMap = Graql.compute().withGraph(graph).cluster().clusterSize(1L).execute();
            assertEquals(0, sizeMap.size());
            memberMap = graph.graql().compute().cluster().members().clusterSize(1L).execute();
            assertEquals(0, memberMap.size());
        }

        addResourceRelations();

        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            sizeMap = graph.graql().compute().cluster().clusterSize(1L).execute();
            assertEquals(5, sizeMap.size());

            memberMap = graph.graql().compute().cluster().members().clusterSize(1L).execute();
            assertEquals(5, memberMap.size());

            sizeMapPersist = graph.graql().compute().cluster().clusterSize(1L).execute();
            assertEquals(5, sizeMapPersist.size());

            sizeMapPersist = graph.graql().compute().cluster().clusterSize(1L).execute();
            assertEquals(5, sizeMapPersist.size());

            memberMapPersist = graph.graql().compute().cluster().members().clusterSize(1L).execute();
            assertEquals(5, memberMapPersist.size());
        }
    }

    @Test
    public void testConnectedComponentImplicitType() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        String aResourceTypeLabel = "aResourceTypeLabel";

        addOntologyAndEntities();
        addResourceRelations();

        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            ResourceType<String> resourceType =
                    graph.putResourceType(aResourceTypeLabel, ResourceType.DataType.STRING);
            graph.getEntityType(thing).resource(resourceType);
            graph.getEntityType(anotherThing).resource(resourceType);
            Resource aResource = resourceType.putResource("blah");
            graph.getEntityType(thing).instances().forEach(instance -> instance.resource(aResource));
            graph.getEntityType(anotherThing).instances().forEach(instance -> instance.resource(aResource));
            graph.commit();
        }

        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            Map<String, Set<String>> result = graph.graql().compute()
                    .cluster().in(thing, anotherThing, aResourceTypeLabel).members().execute();
            assertEquals(1, result.size());
            assertEquals(5, result.values().iterator().next().size());

            assertEquals(1, graph.graql().compute()
                    .cluster().in(thing, anotherThing, aResourceTypeLabel).members().execute().size());
        }
    }

    @Test
    public void testConnectedComponent() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        Map<String, Long> sizeMap;
        Map<String, Set<String>> memberMap;

        // add something, test again
        addOntologyAndEntities();

        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            sizeMap = Graql.compute().withGraph(graph).cluster().execute();
            assertEquals(1, sizeMap.size());
            assertEquals(7L, sizeMap.values().iterator().next().longValue()); // 4 entities, 3 assertions

            memberMap = Graql.compute().withGraph(graph).cluster().in().members().execute();
            assertEquals(1, memberMap.size());
            assertEquals(7, memberMap.values().iterator().next().size());
        }

        // add different resources. This may change existing cluster labels.
        addResourceRelations();

        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            long start = System.currentTimeMillis();
            sizeMap = graph.graql().compute().cluster().execute();
            System.out.println(System.currentTimeMillis() - start + " ms");
            Map<Long, Integer> populationCount00 = new HashMap<>();
            sizeMap.values().forEach(value -> populationCount00.put(value,
                    populationCount00.containsKey(value) ? populationCount00.get(value) + 1 : 1));
            assertEquals(5, populationCount00.get(1L).intValue()); // 5 resources are not connected to anything
            assertEquals(1, populationCount00.get(27L).intValue());

            memberMap = graph.graql().compute().cluster().members().execute();
            assertEquals(6, memberMap.size());
            Map<Integer, Integer> populationCount1 = new HashMap<>();
            memberMap.values().forEach(value -> populationCount1.put(value.size(),
                    populationCount1.containsKey(value.size()) ? populationCount1.get(value.size()) + 1 : 1));
            assertEquals(5, populationCount1.get(1).intValue());
            assertEquals(1, populationCount1.get(27).intValue());

            // test on subtypes. This will change existing cluster labels.
            Set<TypeLabel> subTypes = Sets.newHashSet(thing, anotherThing, resourceType1, resourceType2,
                    resourceType3, resourceType4, resourceType5, resourceType6).stream().map(TypeLabel::of).collect(Collectors.toSet());
            sizeMap = graph.graql().compute().cluster().in(subTypes).execute();
            assertEquals(7, sizeMap.size());
            memberMap = graph.graql().compute().cluster().members().in(subTypes).execute();
            assertEquals(7, memberMap.size());

            String id;
            id = graph.getResourceType(resourceType1).putResource(2.8).asInstance().getId().getValue();
            assertEquals(1L, sizeMap.get(id).longValue());
            id = graph.getResourceType(resourceType2).putResource(-5L).asInstance().getId().getValue();
            assertEquals(1L, sizeMap.get(id).longValue());
            id = graph.getResourceType(resourceType3).putResource(100L).asInstance().getId().getValue();
            assertEquals(1L, sizeMap.get(id).longValue());
            id = graph.getResourceType(resourceType5).putResource(10L).asInstance().getId().getValue();
            assertEquals(1L, sizeMap.get(id).longValue());
            id = graph.getResourceType(resourceType6).putResource(0.8).asInstance().getId().getValue();
            assertEquals(1L, sizeMap.get(id).longValue());
        }
    }

    @Test
    public void testConnectedComponentConcurrency() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        addOntologyAndEntities();

        List<Long> list = new ArrayList<>(4);
        for (long i = 0L; i < 4L; i++) {
            list.add(i);
        }
        GraknSparkComputer.clear();
        list.parallelStream().forEach(i -> {
            try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
                Map<String, Long> sizeMap1 = Graql.compute().withGraph(graph).cluster().execute();
                assertEquals(1, sizeMap1.size());
                assertEquals(7L, sizeMap1.values().iterator().next().longValue());
            }
        });
    }

    private void addOntologyAndEntities() throws GraknValidationException {
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {

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
            entityType1.plays(role1).plays(role2);
            entityType2.plays(role1).plays(role2);
            RelationType relationType = graph.putRelationType(related).relates(role1).relates(role2);

            ConceptId relationId12 = relationType.addRelation()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity2).getId();
            ConceptId relationId23 = relationType.addRelation()
                    .addRolePlayer(role1, entity2)
                    .addRolePlayer(role2, entity3).getId();
            ConceptId relationId24 = relationType.addRelation()
                    .addRolePlayer(role1, entity2)
                    .addRolePlayer(role2, entity4).getId();
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

            RoleType resourceOwner1 = graph.putRoleType(Schema.ImplicitType.HAS_OWNER.getLabel(TypeLabel.of(resourceType1)).getValue());
            RoleType resourceOwner2 = graph.putRoleType(Schema.ImplicitType.HAS_OWNER.getLabel(TypeLabel.of(resourceType2)).getValue());
            RoleType resourceOwner3 = graph.putRoleType(Schema.ImplicitType.HAS_OWNER.getLabel(TypeLabel.of(resourceType3)).getValue());
            RoleType resourceOwner4 = graph.putRoleType(Schema.ImplicitType.HAS_OWNER.getLabel(TypeLabel.of(resourceType4)).getValue());
            RoleType resourceOwner5 = graph.putRoleType(Schema.ImplicitType.HAS_OWNER.getLabel(TypeLabel.of(resourceType5)).getValue());
            RoleType resourceOwner6 = graph.putRoleType(Schema.ImplicitType.HAS_OWNER.getLabel(TypeLabel.of(resourceType6)).getValue());
            RoleType resourceOwner7 = graph.putRoleType(Schema.ImplicitType.HAS_OWNER.getLabel(TypeLabel.of(resourceType7)).getValue());

            RoleType resourceValue1 = graph.putRoleType(Schema.ImplicitType.HAS_VALUE.getLabel(TypeLabel.of(resourceType1)).getValue());
            RoleType resourceValue2 = graph.putRoleType(Schema.ImplicitType.HAS_VALUE.getLabel(TypeLabel.of(resourceType2)).getValue());
            RoleType resourceValue3 = graph.putRoleType(Schema.ImplicitType.HAS_VALUE.getLabel(TypeLabel.of(resourceType3)).getValue());
            RoleType resourceValue4 = graph.putRoleType(Schema.ImplicitType.HAS_VALUE.getLabel(TypeLabel.of(resourceType4)).getValue());
            RoleType resourceValue5 = graph.putRoleType(Schema.ImplicitType.HAS_VALUE.getLabel(TypeLabel.of(resourceType5)).getValue());
            RoleType resourceValue6 = graph.putRoleType(Schema.ImplicitType.HAS_VALUE.getLabel(TypeLabel.of(resourceType6)).getValue());
            RoleType resourceValue7 = graph.putRoleType(Schema.ImplicitType.HAS_VALUE.getLabel(TypeLabel.of(resourceType7)).getValue());

            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(TypeLabel.of(resourceType1)).getValue())
                    .relates(resourceOwner1).relates(resourceValue1);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(TypeLabel.of(resourceType2)).getValue())
                    .relates(resourceOwner2).relates(resourceValue2);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(TypeLabel.of(resourceType3)).getValue())
                    .relates(resourceOwner3).relates(resourceValue3);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(TypeLabel.of(resourceType4)).getValue())
                    .relates(resourceOwner4).relates(resourceValue4);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(TypeLabel.of(resourceType5)).getValue())
                    .relates(resourceOwner5).relates(resourceValue5);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(TypeLabel.of(resourceType6)).getValue())
                    .relates(resourceOwner6).relates(resourceValue6);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(TypeLabel.of(resourceType7)).getValue())
                    .relates(resourceOwner7).relates(resourceValue7);

            entityType1.plays(resourceOwner1)
                    .plays(resourceOwner2)
                    .plays(resourceOwner3)
                    .plays(resourceOwner4)
                    .plays(resourceOwner5)
                    .plays(resourceOwner6)
                    .plays(resourceOwner7);
            entityType2.plays(resourceOwner1)
                    .plays(resourceOwner2)
                    .plays(resourceOwner3)
                    .plays(resourceOwner4)
                    .plays(resourceOwner5)
                    .plays(resourceOwner6)
                    .plays(resourceOwner7);

            resourceTypeList.forEach(resourceType -> resourceType
                    .plays(resourceValue1)
                    .plays(resourceValue2)
                    .plays(resourceValue3)
                    .plays(resourceValue4)
                    .plays(resourceValue5)
                    .plays(resourceValue6)
                    .plays(resourceValue7));

            graph.commit();
        }
    }

    private void addResourceRelations() throws GraknValidationException {
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {

            Entity entity1 = graph.getConcept(entityId1);
            Entity entity2 = graph.getConcept(entityId2);
            Entity entity3 = graph.getConcept(entityId3);
            Entity entity4 = graph.getConcept(entityId4);

            RoleType resourceOwner1 = graph.getType(Schema.ImplicitType.HAS_OWNER.getLabel(TypeLabel.of(resourceType1)));
            RoleType resourceOwner2 = graph.getType(Schema.ImplicitType.HAS_OWNER.getLabel(TypeLabel.of(resourceType2)));
            RoleType resourceOwner3 = graph.getType(Schema.ImplicitType.HAS_OWNER.getLabel(TypeLabel.of(resourceType3)));
            RoleType resourceOwner4 = graph.getType(Schema.ImplicitType.HAS_OWNER.getLabel(TypeLabel.of(resourceType4)));
            RoleType resourceOwner5 = graph.getType(Schema.ImplicitType.HAS_OWNER.getLabel(TypeLabel.of(resourceType5)));
            RoleType resourceOwner6 = graph.getType(Schema.ImplicitType.HAS_OWNER.getLabel(TypeLabel.of(resourceType6)));

            RoleType resourceValue1 = graph.getType(Schema.ImplicitType.HAS_VALUE.getLabel(TypeLabel.of(resourceType1)));
            RoleType resourceValue2 = graph.getType(Schema.ImplicitType.HAS_VALUE.getLabel(TypeLabel.of(resourceType2)));
            RoleType resourceValue3 = graph.getType(Schema.ImplicitType.HAS_VALUE.getLabel(TypeLabel.of(resourceType3)));
            RoleType resourceValue4 = graph.getType(Schema.ImplicitType.HAS_VALUE.getLabel(TypeLabel.of(resourceType4)));
            RoleType resourceValue5 = graph.getType(Schema.ImplicitType.HAS_VALUE.getLabel(TypeLabel.of(resourceType5)));
            RoleType resourceValue6 = graph.getType(Schema.ImplicitType.HAS_VALUE.getLabel(TypeLabel.of(resourceType6)));

            RelationType relationType1 = graph.getType(Schema.ImplicitType.HAS.getLabel(TypeLabel.of(resourceType1)));
            relationType1.addRelation()
                    .addRolePlayer(resourceOwner1, entity1)
                    .addRolePlayer(resourceValue1, graph.getResourceType(resourceType1).putResource(1.2));
            relationType1.addRelation()
                    .addRolePlayer(resourceOwner1, entity1)
                    .addRolePlayer(resourceValue1, graph.getResourceType(resourceType1).putResource(1.5));
            relationType1.addRelation()
                    .addRolePlayer(resourceOwner1, entity3)
                    .addRolePlayer(resourceValue1, graph.getResourceType(resourceType1).putResource(1.8));

            RelationType relationType2 = graph.getType(Schema.ImplicitType.HAS.getLabel(TypeLabel.of(resourceType2)));
            relationType2.addRelation()
                    .addRolePlayer(resourceOwner2, entity1)
                    .addRolePlayer(resourceValue2, graph.getResourceType(resourceType2).putResource(4L));
            relationType2.addRelation()
                    .addRolePlayer(resourceOwner2, entity1)
                    .addRolePlayer(resourceValue2, graph.getResourceType(resourceType2).putResource(-1L));
            relationType2.addRelation()
                    .addRolePlayer(resourceOwner2, entity4)
                    .addRolePlayer(resourceValue2, graph.getResourceType(resourceType2).putResource(0L));

            RelationType relationType5 = graph.getType(Schema.ImplicitType.HAS.getLabel(TypeLabel.of(resourceType5)));
            relationType5.addRelation()
                    .addRolePlayer(resourceOwner5, entity1)
                    .addRolePlayer(resourceValue5, graph.getResourceType(resourceType5).putResource(-7L));
            relationType5.addRelation()
                    .addRolePlayer(resourceOwner5, entity2)
                    .addRolePlayer(resourceValue5, graph.getResourceType(resourceType5).putResource(-7L));
            relationType5.addRelation()
                    .addRolePlayer(resourceOwner5, entity4)
                    .addRolePlayer(resourceValue5, graph.getResourceType(resourceType5).putResource(-7L));

            RelationType relationType6 = graph.getType(Schema.ImplicitType.HAS.getLabel(TypeLabel.of(resourceType6)));
            relationType6.addRelation()
                    .addRolePlayer(resourceOwner6, entity1)
                    .addRolePlayer(resourceValue6, graph.getResourceType(resourceType6).putResource(7.5));
            relationType6.addRelation()
                    .addRolePlayer(resourceOwner6, entity2)
                    .addRolePlayer(resourceValue6, graph.getResourceType(resourceType6).putResource(7.5));
            relationType6.addRelation()
                    .addRolePlayer(resourceOwner6, entity4)
                    .addRolePlayer(resourceValue6, graph.getResourceType(resourceType6).putResource(7.5));

            // some resources in, but not connect them to any instances
            graph.getResourceType(resourceType1).putResource(2.8);
            graph.getResourceType(resourceType2).putResource(-5L);
            graph.getResourceType(resourceType3).putResource(100L);
            graph.getResourceType(resourceType5).putResource(10L);
            graph.getResourceType(resourceType6).putResource(0.8);

            graph.commit();
        }
    }
}
