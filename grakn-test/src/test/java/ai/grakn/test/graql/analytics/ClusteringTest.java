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
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.graql.Graql;
import ai.grakn.test.EngineContext;
import ai.grakn.test.GraknTestSetup;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClusteringTest {
    private static final String thing = "thingy";
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

    @ClassRule
    public static final EngineContext context = EngineContext.startInMemoryServer();
    private GraknSession factory;

    @Before
    public void setUp() {
        factory = context.factoryWithNewKeyspace();
    }

    @Test
    public void testConnectedComponentOnEmptyGraph() throws Exception {
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
            sizeMap = graph.graql().compute().cluster().execute();
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
            Set<Label> subTypes = Sets.newHashSet(thing, anotherThing, resourceType1, resourceType2,
                    resourceType3, resourceType4, resourceType5, resourceType6).stream().map(Label::of).collect(Collectors.toSet());
            sizeMap = graph.graql().compute().cluster().in(subTypes).execute();
            assertEquals(7, sizeMap.size());
            memberMap = graph.graql().compute().cluster().members().in(subTypes).execute();
            assertEquals(7, memberMap.size());

            String id;
            id = graph.getResourceType(resourceType1).putResource(2.8).getId().getValue();
            assertEquals(1L, sizeMap.get(id).longValue());
            id = graph.getResourceType(resourceType2).putResource(-5L).getId().getValue();
            assertEquals(1L, sizeMap.get(id).longValue());
            id = graph.getResourceType(resourceType3).putResource(100L).getId().getValue();
            assertEquals(1L, sizeMap.get(id).longValue());
            id = graph.getResourceType(resourceType5).putResource(10L).getId().getValue();
            assertEquals(1L, sizeMap.get(id).longValue());
            id = graph.getResourceType(resourceType6).putResource(0.8).getId().getValue();
            assertEquals(1L, sizeMap.get(id).longValue());
        }
    }

    @Test
    public void testConnectedComponentConcurrency() throws Exception {
        addOntologyAndEntities();

        List<Long> list = new ArrayList<>(4);
        long workerNumber = 4L;
        if (GraknTestSetup.usingTinker()) workerNumber = 1L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }

        Set<Map<String, Long>> result = list.parallelStream().map(i -> {
            try (GraknGraph graph = factory.open(GraknTxType.READ)) {
                return Graql.compute().withGraph(graph).cluster().execute();
            }
        }).collect(Collectors.toSet());
        result.forEach(map -> {
            assertEquals(1, map.size());
            assertEquals(7L, map.values().iterator().next().longValue());
        });
    }

    private void addOntologyAndEntities() throws InvalidGraphException {
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

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
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
            List<ConceptId> instanceIds = Lists.newArrayList(entityId1, entityId2, entityId3, entityId4,
                    relationId12, relationId23, relationId24);

            List<ResourceType> resourceTypeList = new ArrayList<>();
            resourceTypeList.add(graph.putResourceType(resourceType1, ResourceType.DataType.DOUBLE));
            resourceTypeList.add(graph.putResourceType(resourceType2, ResourceType.DataType.LONG));
            resourceTypeList.add(graph.putResourceType(resourceType3, ResourceType.DataType.LONG));
            resourceTypeList.add(graph.putResourceType(resourceType4, ResourceType.DataType.STRING));
            resourceTypeList.add(graph.putResourceType(resourceType5, ResourceType.DataType.LONG));
            resourceTypeList.add(graph.putResourceType(resourceType6, ResourceType.DataType.DOUBLE));
            resourceTypeList.add(graph.putResourceType(resourceType7, ResourceType.DataType.DOUBLE));

            Role resourceOwner1 = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType1)).getValue());
            Role resourceOwner2 = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType2)).getValue());
            Role resourceOwner3 = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType3)).getValue());
            Role resourceOwner4 = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType4)).getValue());
            Role resourceOwner5 = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType5)).getValue());
            Role resourceOwner6 = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType6)).getValue());
            Role resourceOwner7 = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType7)).getValue());

            Role resourceValue1 = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType1)).getValue());
            Role resourceValue2 = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType2)).getValue());
            Role resourceValue3 = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType3)).getValue());
            Role resourceValue4 = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType4)).getValue());
            Role resourceValue5 = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType5)).getValue());
            Role resourceValue6 = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType6)).getValue());
            Role resourceValue7 = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType7)).getValue());

            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType1)).getValue())
                    .relates(resourceOwner1).relates(resourceValue1);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType2)).getValue())
                    .relates(resourceOwner2).relates(resourceValue2);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType3)).getValue())
                    .relates(resourceOwner3).relates(resourceValue3);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType4)).getValue())
                    .relates(resourceOwner4).relates(resourceValue4);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType5)).getValue())
                    .relates(resourceOwner5).relates(resourceValue5);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType6)).getValue())
                    .relates(resourceOwner6).relates(resourceValue6);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType7)).getValue())
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

    private void addResourceRelations() throws InvalidGraphException {
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {

            Entity entity1 = graph.getConcept(entityId1);
            Entity entity2 = graph.getConcept(entityId2);
            Entity entity3 = graph.getConcept(entityId3);
            Entity entity4 = graph.getConcept(entityId4);

            Role resourceOwner1 = graph.getOntologyConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType1)));
            Role resourceOwner2 = graph.getOntologyConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType2)));
            Role resourceOwner3 = graph.getOntologyConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType3)));
            Role resourceOwner4 = graph.getOntologyConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType4)));
            Role resourceOwner5 = graph.getOntologyConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType5)));
            Role resourceOwner6 = graph.getOntologyConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType6)));

            Role resourceValue1 = graph.getOntologyConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType1)));
            Role resourceValue2 = graph.getOntologyConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType2)));
            Role resourceValue3 = graph.getOntologyConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType3)));
            Role resourceValue4 = graph.getOntologyConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType4)));
            Role resourceValue5 = graph.getOntologyConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType5)));
            Role resourceValue6 = graph.getOntologyConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType6)));

            RelationType relationType1 = graph.getOntologyConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType1)));
            relationType1.addRelation()
                    .addRolePlayer(resourceOwner1, entity1)
                    .addRolePlayer(resourceValue1, graph.getResourceType(resourceType1).putResource(1.2));
            relationType1.addRelation()
                    .addRolePlayer(resourceOwner1, entity1)
                    .addRolePlayer(resourceValue1, graph.getResourceType(resourceType1).putResource(1.5));
            relationType1.addRelation()
                    .addRolePlayer(resourceOwner1, entity3)
                    .addRolePlayer(resourceValue1, graph.getResourceType(resourceType1).putResource(1.8));

            RelationType relationType2 = graph.getOntologyConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType2)));
            relationType2.addRelation()
                    .addRolePlayer(resourceOwner2, entity1)
                    .addRolePlayer(resourceValue2, graph.getResourceType(resourceType2).putResource(4L));
            relationType2.addRelation()
                    .addRolePlayer(resourceOwner2, entity1)
                    .addRolePlayer(resourceValue2, graph.getResourceType(resourceType2).putResource(-1L));
            relationType2.addRelation()
                    .addRolePlayer(resourceOwner2, entity4)
                    .addRolePlayer(resourceValue2, graph.getResourceType(resourceType2).putResource(0L));

            RelationType relationType5 = graph.getOntologyConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType5)));
            relationType5.addRelation()
                    .addRolePlayer(resourceOwner5, entity1)
                    .addRolePlayer(resourceValue5, graph.getResourceType(resourceType5).putResource(-7L));
            relationType5.addRelation()
                    .addRolePlayer(resourceOwner5, entity2)
                    .addRolePlayer(resourceValue5, graph.getResourceType(resourceType5).putResource(-7L));
            relationType5.addRelation()
                    .addRolePlayer(resourceOwner5, entity4)
                    .addRolePlayer(resourceValue5, graph.getResourceType(resourceType5).putResource(-7L));

            RelationType relationType6 = graph.getOntologyConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType6)));
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
