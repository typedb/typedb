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
            Map<String, Long> sizeMap = Graql.compute().withTx(graph).cluster().includeAttribute().execute();
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

        addSchemaAndEntities();

        try (GraknTx graph = factory.open(GraknTxType.WRITE)) {
            sizeMap = Graql.compute().withTx(graph).cluster().includeAttribute().clusterSize(1L).execute();
            assertEquals(0, sizeMap.size());
            memberMap = graph.graql().compute().cluster().members().clusterSize(1L).execute();
            assertEquals(0, memberMap.size());
        }

        addResourceRelations();

        try (GraknTx graph = factory.open(GraknTxType.READ)) {
            sizeMap = graph.graql().compute().cluster().includeAttribute().clusterSize(1L).execute();
            assertEquals(5, sizeMap.size());

            memberMap = graph.graql().compute().cluster().members().clusterSize(1L).execute();
            assertEquals(0, memberMap.size());
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
                    .cluster().in(thing, anotherThing, aResourceTypeLabel,
                            Schema.ImplicitType.HAS.getLabel(aResourceTypeLabel).getValue())
                    .members().execute();
            assertEquals(1, result.size());
            assertEquals(5, result.values().iterator().next().size());

            assertEquals(1, graph.graql().compute().cluster().includeAttribute()
                    .in(thing, anotherThing, aResourceTypeLabel,
                            Schema.ImplicitType.HAS.getLabel(aResourceTypeLabel).getValue())
                    .includeAttribute().members().execute().size());
        }
    }

    @Test
    public void testConnectedComponent() throws Exception {
        Map<String, Long> sizeMap;
        Map<String, Set<String>> memberMap;

        // add something, test again
        addSchemaAndEntities();

        try (GraknTx graph = factory.open(GraknTxType.READ)) {
            sizeMap = Graql.compute().withTx(graph).cluster().includeAttribute().execute();
            assertEquals(1, sizeMap.size());
            assertEquals(7L, sizeMap.values().iterator().next().longValue()); // 4 entities, 3 assertions

            memberMap = Graql.compute().withTx(graph).cluster().in().members().execute();
            assertEquals(1, memberMap.size());
            assertEquals(7, memberMap.values().iterator().next().size());
        }

        // add different resources. This may change existing cluster labels.
        addResourceRelations();

        try (GraknTx graph = factory.open(GraknTxType.READ)) {
            sizeMap = graph.graql().compute().cluster().includeAttribute().execute();
            Map<Long, Integer> populationCount00 = new HashMap<>();
            sizeMap.values().forEach(value -> populationCount00.put(value,
                    populationCount00.containsKey(value) ? populationCount00.get(value) + 1 : 1));
            // 5 resources are not connected to anything
            assertEquals(5, populationCount00.get(1L).intValue());
            assertEquals(1, populationCount00.get(15L).intValue());

            memberMap = graph.graql().compute().cluster().members().execute();
            assertEquals(1, memberMap.size());
            Map<Integer, Integer> populationCount1 = new HashMap<>();
            memberMap.values().forEach(value -> populationCount1.put(value.size(),
                    populationCount1.containsKey(value.size()) ? populationCount1.get(value.size()) + 1 : 1));
            assertEquals(1, populationCount1.get(7).intValue());

            // test on subtypes. This will change existing cluster labels.
            Set<Label> subTypes = Sets.newHashSet(thing, anotherThing, resourceType1, resourceType2,
                    resourceType3, resourceType4, resourceType5, resourceType6)
                    .stream().map(Label::of).collect(Collectors.toSet());
            sizeMap = graph.graql().compute().cluster().in(subTypes).execute();
            assertEquals(17, sizeMap.size()); // No relationships, so this is the entity count;
            memberMap = graph.graql().compute().cluster().members().in(subTypes).execute();
            assertEquals(17, memberMap.size());
        }
    }

    @Test
    public void testConnectedComponentConcurrency() throws Exception {
        assumeFalse(GraknTestSetup.usingTinker());

        addSchemaAndEntities();
        addResourceRelations();

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

            attributeTypeList.forEach(attributeType -> {
                entityType1.attribute(attributeType);
                entityType2.attribute(attributeType);
            });

            graph.commit();
        }
    }

    private void addResourceRelations() throws InvalidKBException {
        try (GraknTx graph = factory.open(GraknTxType.WRITE)) {

            Entity entity1 = graph.getConcept(entityId1);
            Entity entity2 = graph.getConcept(entityId2);
            Entity entity3 = graph.getConcept(entityId3);
            Entity entity4 = graph.getConcept(entityId4);

            entity1.attribute(graph.getAttributeType(resourceType1).putAttribute(1.2))
                    .attribute(graph.getAttributeType(resourceType1).putAttribute(1.5))
                    .attribute(graph.getAttributeType(resourceType2).putAttribute(4L))
                    .attribute(graph.getAttributeType(resourceType2).putAttribute(-1L))
                    .attribute(graph.getAttributeType(resourceType5).putAttribute(-7L))
                    .attribute(graph.getAttributeType(resourceType6).putAttribute(7.5));

            entity2.attribute(graph.getAttributeType(resourceType5).putAttribute(-7L))
                    .attribute(graph.getAttributeType(resourceType6).putAttribute(7.5));

            entity3.attribute(graph.getAttributeType(resourceType1).putAttribute(1.8));

            entity4.attribute(graph.getAttributeType(resourceType2).putAttribute(0L))
                    .attribute(graph.getAttributeType(resourceType5).putAttribute(-7L))
                    .attribute(graph.getAttributeType(resourceType6).putAttribute(7.5));

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
