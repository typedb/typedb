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

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.Graql;
import ai.grakn.test.EngineContext;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.util.Schema;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assume.assumeFalse;

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
    public static final EngineContext context = EngineContext.inMemoryServer();
    private GraknSession factory;

    @Before
    public void setUp() {
        factory = context.sessionWithNewKeyspace();
    }

    @Test
    public void testConnectedComponentOnEmptyGraph() throws Exception {
        try (GraknTx graph = factory.open(GraknTxType.WRITE)) {
            // test on an empty rule.graph()
            Map<String, Long> sizeMap = Graql.compute().withTx(graph).cluster().execute();
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

        addSchemaAndEntities();

        try (GraknTx graph = factory.open(GraknTxType.WRITE)) {
            sizeMap = Graql.compute().withTx(graph).cluster().clusterSize(1L).execute();
            assertEquals(0, sizeMap.size());
            memberMap = graph.graql().compute().cluster().members().clusterSize(1L).execute();
            assertEquals(0, memberMap.size());
        }

        addResourceRelations();

        try (GraknTx graph = factory.open(GraknTxType.READ)) {
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

        addSchemaAndEntities();
        addResourceRelations();

        try (GraknTx graph = factory.open(GraknTxType.WRITE)) {
            AttributeType<String> attributeType =
                    graph.putAttributeType(aResourceTypeLabel, AttributeType.DataType.STRING);
            graph.getEntityType(thing).attribute(attributeType);
            graph.getEntityType(anotherThing).attribute(attributeType);
            Attribute aAttribute = attributeType.putAttribute("blah");
            graph.getEntityType(thing).instances().forEach(instance -> instance.attribute(aAttribute));
            graph.getEntityType(anotherThing).instances().forEach(instance -> instance.attribute(aAttribute));
            graph.commit();
        }

        try (GraknTx graph = factory.open(GraknTxType.READ)) {
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
        addSchemaAndEntities();

        try (GraknTx graph = factory.open(GraknTxType.READ)) {
            sizeMap = Graql.compute().withTx(graph).cluster().execute();
            assertEquals(1, sizeMap.size());
            assertEquals(7L, sizeMap.values().iterator().next().longValue()); // 4 entities, 3 assertions

            memberMap = Graql.compute().withTx(graph).cluster().in().members().execute();
            assertEquals(1, memberMap.size());
            assertEquals(7, memberMap.values().iterator().next().size());
        }

        // add different resources. This may change existing cluster labels.
        addResourceRelations();

        try (GraknTx graph = factory.open(GraknTxType.READ)) {
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
        }

        try (GraknTx graph = factory.open(GraknTxType.WRITE)) {
            String id;
            id = graph.getAttributeType(resourceType1).putAttribute(2.8).getId().getValue();
            assertEquals(1L, sizeMap.get(id).longValue());
            id = graph.getAttributeType(resourceType2).putAttribute(-5L).getId().getValue();
            assertEquals(1L, sizeMap.get(id).longValue());
            id = graph.getAttributeType(resourceType3).putAttribute(100L).getId().getValue();
            assertEquals(1L, sizeMap.get(id).longValue());
            id = graph.getAttributeType(resourceType5).putAttribute(10L).getId().getValue();
            assertEquals(1L, sizeMap.get(id).longValue());
            id = graph.getAttributeType(resourceType6).putAttribute(0.8).getId().getValue();
            assertEquals(1L, sizeMap.get(id).longValue());
        }
    }

    @Test
    public void testConnectedComponentConcurrency() throws Exception {
        assumeFalse(GraknTestSetup.usingTinker());

        addSchemaAndEntities();

        List<Long> list = new ArrayList<>(4);
        long workerNumber = 4L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }

        Set<Map<String, Long>> result = list.parallelStream().map(i -> {
            try (GraknTx graph = factory.open(GraknTxType.READ)) {
                return Graql.compute().withTx(graph).cluster().execute();
            }
        }).collect(Collectors.toSet());
        result.forEach(map -> {
            assertEquals(1, map.size());
            assertEquals(7L, map.values().iterator().next().longValue());
        });
    }

    private void addSchemaAndEntities() throws InvalidKBException {
        try (GraknTx graph = factory.open(GraknTxType.WRITE)) {

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
            RelationshipType relationshipType = graph.putRelationshipType(related).relates(role1).relates(role2);

            ConceptId relationId12 = relationshipType.addRelationship()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity2).getId();
            ConceptId relationId23 = relationshipType.addRelationship()
                    .addRolePlayer(role1, entity2)
                    .addRolePlayer(role2, entity3).getId();
            ConceptId relationId24 = relationshipType.addRelationship()
                    .addRolePlayer(role1, entity2)
                    .addRolePlayer(role2, entity4).getId();
            List<ConceptId> instanceIds = Lists.newArrayList(entityId1, entityId2, entityId3, entityId4,
                    relationId12, relationId23, relationId24);

            List<AttributeType> attributeTypeList = new ArrayList<>();
            attributeTypeList.add(graph.putAttributeType(resourceType1, AttributeType.DataType.DOUBLE));
            attributeTypeList.add(graph.putAttributeType(resourceType2, AttributeType.DataType.LONG));
            attributeTypeList.add(graph.putAttributeType(resourceType3, AttributeType.DataType.LONG));
            attributeTypeList.add(graph.putAttributeType(resourceType4, AttributeType.DataType.STRING));
            attributeTypeList.add(graph.putAttributeType(resourceType5, AttributeType.DataType.LONG));
            attributeTypeList.add(graph.putAttributeType(resourceType6, AttributeType.DataType.DOUBLE));
            attributeTypeList.add(graph.putAttributeType(resourceType7, AttributeType.DataType.DOUBLE));

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

            graph.putRelationshipType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType1)).getValue())
                    .relates(resourceOwner1).relates(resourceValue1);
            graph.putRelationshipType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType2)).getValue())
                    .relates(resourceOwner2).relates(resourceValue2);
            graph.putRelationshipType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType3)).getValue())
                    .relates(resourceOwner3).relates(resourceValue3);
            graph.putRelationshipType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType4)).getValue())
                    .relates(resourceOwner4).relates(resourceValue4);
            graph.putRelationshipType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType5)).getValue())
                    .relates(resourceOwner5).relates(resourceValue5);
            graph.putRelationshipType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType6)).getValue())
                    .relates(resourceOwner6).relates(resourceValue6);
            graph.putRelationshipType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType7)).getValue())
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

            attributeTypeList.forEach(resourceType -> resourceType
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

    private void addResourceRelations() throws InvalidKBException {
        try (GraknTx graph = factory.open(GraknTxType.WRITE)) {

            Entity entity1 = graph.getConcept(entityId1);
            Entity entity2 = graph.getConcept(entityId2);
            Entity entity3 = graph.getConcept(entityId3);
            Entity entity4 = graph.getConcept(entityId4);

            Role resourceOwner1 = graph.getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType1)));
            Role resourceOwner2 = graph.getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType2)));
            Role resourceOwner3 = graph.getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType3)));
            Role resourceOwner4 = graph.getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType4)));
            Role resourceOwner5 = graph.getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType5)));
            Role resourceOwner6 = graph.getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType6)));

            Role resourceValue1 = graph.getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType1)));
            Role resourceValue2 = graph.getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType2)));
            Role resourceValue3 = graph.getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType3)));
            Role resourceValue4 = graph.getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType4)));
            Role resourceValue5 = graph.getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType5)));
            Role resourceValue6 = graph.getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType6)));

            RelationshipType relationshipType1 = graph.getSchemaConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType1)));
            relationshipType1.addRelationship()
                    .addRolePlayer(resourceOwner1, entity1)
                    .addRolePlayer(resourceValue1, graph.getAttributeType(resourceType1).putAttribute(1.2));
            relationshipType1.addRelationship()
                    .addRolePlayer(resourceOwner1, entity1)
                    .addRolePlayer(resourceValue1, graph.getAttributeType(resourceType1).putAttribute(1.5));
            relationshipType1.addRelationship()
                    .addRolePlayer(resourceOwner1, entity3)
                    .addRolePlayer(resourceValue1, graph.getAttributeType(resourceType1).putAttribute(1.8));

            RelationshipType relationshipType2 = graph.getSchemaConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType2)));
            relationshipType2.addRelationship()
                    .addRolePlayer(resourceOwner2, entity1)
                    .addRolePlayer(resourceValue2, graph.getAttributeType(resourceType2).putAttribute(4L));
            relationshipType2.addRelationship()
                    .addRolePlayer(resourceOwner2, entity1)
                    .addRolePlayer(resourceValue2, graph.getAttributeType(resourceType2).putAttribute(-1L));
            relationshipType2.addRelationship()
                    .addRolePlayer(resourceOwner2, entity4)
                    .addRolePlayer(resourceValue2, graph.getAttributeType(resourceType2).putAttribute(0L));

            RelationshipType relationshipType5 = graph.getSchemaConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType5)));
            relationshipType5.addRelationship()
                    .addRolePlayer(resourceOwner5, entity1)
                    .addRolePlayer(resourceValue5, graph.getAttributeType(resourceType5).putAttribute(-7L));
            relationshipType5.addRelationship()
                    .addRolePlayer(resourceOwner5, entity2)
                    .addRolePlayer(resourceValue5, graph.getAttributeType(resourceType5).putAttribute(-7L));
            relationshipType5.addRelationship()
                    .addRolePlayer(resourceOwner5, entity4)
                    .addRolePlayer(resourceValue5, graph.getAttributeType(resourceType5).putAttribute(-7L));

            RelationshipType relationshipType6 = graph.getSchemaConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType6)));
            relationshipType6.addRelationship()
                    .addRolePlayer(resourceOwner6, entity1)
                    .addRolePlayer(resourceValue6, graph.getAttributeType(resourceType6).putAttribute(7.5));
            relationshipType6.addRelationship()
                    .addRolePlayer(resourceOwner6, entity2)
                    .addRolePlayer(resourceValue6, graph.getAttributeType(resourceType6).putAttribute(7.5));
            relationshipType6.addRelationship()
                    .addRolePlayer(resourceOwner6, entity4)
                    .addRolePlayer(resourceValue6, graph.getAttributeType(resourceType6).putAttribute(7.5));

            // some resources in, but not connect them to any instances
            graph.getAttributeType(resourceType1).putAttribute(2.8);
            graph.getAttributeType(resourceType2).putAttribute(-5L);
            graph.getAttributeType(resourceType3).putAttribute(100L);
            graph.getAttributeType(resourceType5).putAttribute(10L);
            graph.getAttributeType(resourceType6).putAttribute(0.8);

            graph.commit();
        }
    }
}
