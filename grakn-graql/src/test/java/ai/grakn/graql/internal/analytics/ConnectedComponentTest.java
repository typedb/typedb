/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.analytics;

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
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.Graql;
import ai.grakn.test.rule.SessionContext;
import ai.grakn.util.GraknTestUtil;
import ai.grakn.util.Schema;
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

import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.CONNECTED_COMPONENT;
import static ai.grakn.util.GraqlSyntax.Compute.Argument.contains;
import static ai.grakn.util.GraqlSyntax.Compute.Argument.members;
import static ai.grakn.util.GraqlSyntax.Compute.Argument.size;
import static ai.grakn.util.GraqlSyntax.Compute.Method.CLUSTER;
import static ai.grakn.util.GraqlSyntax.Compute.Method.COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class ConnectedComponentTest {
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
    private ConceptId aDisconnectedAttribute;

    public GraknSession session;

    @ClassRule
    public final static SessionContext sessionContext = SessionContext.create();

    @Before
    public void setUp() {
        session = sessionContext.newSession();
    }

    @Test
    public void testNullSourceIdIsIgnored() {
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT).where(contains(null)).execute();
        }

        addSchemaAndEntities();
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT).in(thing).where(contains(null)).execute();
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testSourceDoesNotExistInSubGraph() {
        addSchemaAndEntities();
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT).in(thing).where(contains(entityId4)).execute();
        }
    }

    @Test
    public void testConnectedComponentOnEmptyGraph() {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            // test on an empty rule.graph()
            List<Long> sizeList =
                    Graql.compute(CLUSTER).withTx(graph).using(CONNECTED_COMPONENT)
                            .includeAttributes(true).execute().getClusterSizes().get();
            assertTrue(sizeList.isEmpty());
            Set<Set<ConceptId>> membersList = graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT)
                    .where(members(true)).execute().getClusters().get();
            assertTrue(membersList.isEmpty());
            assertEquals(0L, graph.graql().compute(COUNT).execute().getNumber().get().longValue());
        }
    }

    @Test
    public void testConnectedComponentSize() {
        List<Long> sizeList;
        Set<Set<ConceptId>> membersSet;

        addSchemaAndEntities();

        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            sizeList = Graql.compute(CLUSTER).withTx(graph).using(CONNECTED_COMPONENT)
                    .includeAttributes(true).where(size(1L)).execute().getClusterSizes().get();
            assertEquals(0, sizeList.size());

            membersSet = graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT)
                    .where(members(true)).where(size(1L)).execute().getClusters().get();
            assertEquals(0, membersSet.size());
        }

        addResourceRelations();

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            sizeList = graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT)
                    .includeAttributes(true).where(size(1L)).execute().getClusterSizes().get();
            assertEquals(5, sizeList.size());

            sizeList = graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT)
                    .includeAttributes(true).where(size(1L), contains(entityId1)).execute().getClusterSizes().get();
            assertEquals(0, sizeList.size());

            membersSet = graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT)
                    .where(members(true), size(1L)).execute().getClusters().get();
            assertEquals(0, membersSet.size());

            membersSet = graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT)
                    .where(contains(entityId4), members(true), size(1L)).execute()
                    .getClusters().get();
            assertEquals(0, membersSet.size());
        }
    }

    @Test
    public void testConnectedComponentImplicitType() {
        String aResourceTypeLabel = "aResourceTypeLabel";

        addSchemaAndEntities();
        addResourceRelations();

        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            AttributeType<String> attributeType =
                    graph.putAttributeType(aResourceTypeLabel, AttributeType.DataType.STRING);
            graph.getEntityType(thing).has(attributeType);
            graph.getEntityType(anotherThing).has(attributeType);
            Attribute aAttribute = attributeType.create("blah");
            graph.getEntityType(thing).instances().forEach(instance -> instance.has(aAttribute));
            graph.getEntityType(anotherThing).instances().forEach(instance -> instance.has(aAttribute));
            graph.commit();
        }

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            Set<Set<ConceptId>> result = graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT)
                    .in(thing, anotherThing, aResourceTypeLabel, Schema.ImplicitType.HAS.getLabel(aResourceTypeLabel).getValue())
                    .where(members(true)).execute()
                    .getClusters().get();
            assertEquals(1, result.size());
            assertEquals(5, result.iterator().next().size());

            result = graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT)
                    .in(thing, anotherThing, aResourceTypeLabel, Schema.ImplicitType.HAS.getLabel(aResourceTypeLabel).getValue())
                    .where(members(true), contains(entityId2)).execute()
                    .getClusters().get();
            assertEquals(1, result.size());
            assertEquals(5, result.iterator().next().size());

            assertEquals(1, graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT).includeAttributes(true)
                    .in(thing, anotherThing, aResourceTypeLabel, Schema.ImplicitType.HAS.getLabel(aResourceTypeLabel).getValue())
                    .includeAttributes(true).where(members(true)).execute()
                    .getClusters().get().size());
        }
    }

    @Test
    public void testConnectedComponent() {
        List<Long> sizeList;
        Set<Set<ConceptId>> membersSet;

        addSchemaAndEntities();

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            sizeList = Graql.compute(CLUSTER).withTx(graph).using(CONNECTED_COMPONENT).includeAttributes(true).execute()
                    .getClusterSizes().get();
            assertEquals(1, sizeList.size());
            assertEquals(7L, sizeList.iterator().next().longValue()); // 4 entities, 3 assertions

            sizeList = Graql.compute(CLUSTER).withTx(graph).using(CONNECTED_COMPONENT)
                    .where(contains(entityId1)).includeAttributes(true).execute()
                    .getClusterSizes().get();
            assertEquals(1, sizeList.size());
            assertEquals(7L, sizeList.iterator().next().longValue());

            membersSet = Graql.compute(CLUSTER).withTx(graph).using(CONNECTED_COMPONENT).where(members(true)).execute()
                    .getClusters().get();
            assertEquals(1, membersSet.size());
            assertEquals(7, membersSet.iterator().next().size());

            membersSet = Graql.compute(CLUSTER).withTx(graph).using(CONNECTED_COMPONENT)
                    .where(contains(entityId4), members(true)).execute()
                    .getClusters().get();
            assertEquals(1, membersSet.size());
            assertEquals(7, membersSet.iterator().next().size());
        }

        // add different resources. This may change existing cluster labels.
        addResourceRelations();

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            sizeList = graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT).includeAttributes(true).execute()
                    .getClusterSizes().get();
            Map<Long, Integer> populationCount00 = new HashMap<>();
            sizeList.forEach(value -> populationCount00.put(value,
                    populationCount00.containsKey(value) ? populationCount00.get(value) + 1 : 1));
            // 5 resources are not connected to anything
            assertEquals(5, populationCount00.get(1L).intValue());
            assertEquals(1, populationCount00.get(15L).intValue());

            sizeList = graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT)
                    .where(contains(aDisconnectedAttribute)).includeAttributes(true).execute()
                    .getClusterSizes().get();
            assertEquals(1, sizeList.size());

            membersSet = graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT).where(members(true)).execute()
                    .getClusters().get();
            assertEquals(1, membersSet.size());
            Map<Integer, Integer> populationCount1 = new HashMap<>();
            membersSet.forEach(value -> populationCount1.put(value.size(),
                    populationCount1.containsKey(value.size()) ? populationCount1.get(value.size()) + 1 : 1));
            assertEquals(1, populationCount1.get(7).intValue());

            // test on subtypes. This will change existing cluster labels.
            Set<Label> subTypes = Sets.newHashSet(thing, anotherThing, resourceType1, resourceType2,
                    resourceType3, resourceType4, resourceType5, resourceType6)
                    .stream().map(Label::of).collect(Collectors.toSet());
            sizeList = graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT).in(subTypes).execute()
                    .getClusterSizes().get();
            assertEquals(17, sizeList.size()); // No relationships, so this is the entity count;
            membersSet = graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT).where(members(true)).in(subTypes).execute()
                    .getClusters().get();
            assertEquals(17, membersSet.size());
        }
    }

    @Test
    public void testConnectedComponentConcurrency() {
        assumeFalse(GraknTestUtil.usingTinker());

        addSchemaAndEntities();
        addResourceRelations();

        List<Long> list = new ArrayList<>(4);
        long workerNumber = 4L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }

        Set<List<Long>> result = list.parallelStream().map(i -> {
            try (GraknTx graph = session.transaction(GraknTxType.READ)) {
                return Graql.compute(CLUSTER).withTx(graph).using(CONNECTED_COMPONENT).execute().getClusterSizes().get();
            }
        }).collect(Collectors.toSet());
        result.forEach(map -> {
            assertEquals(1, map.size());
            assertEquals(7L, map.iterator().next().longValue());
        });
    }

    private void addSchemaAndEntities() throws InvalidKBException {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {

            EntityType entityType1 = graph.putEntityType(thing);
            EntityType entityType2 = graph.putEntityType(anotherThing);

            Entity entity1 = entityType1.create();
            Entity entity2 = entityType1.create();
            Entity entity3 = entityType1.create();
            Entity entity4 = entityType2.create();
            entityId1 = entity1.id();
            entityId2 = entity2.id();
            entityId3 = entity3.id();
            entityId4 = entity4.id();

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
            entityType1.plays(role1).plays(role2);
            entityType2.plays(role1).plays(role2);
            RelationshipType relationshipType = graph.putRelationshipType(related).relates(role1).relates(role2);

            relationshipType.create()
                    .assign(role1, entity1)
                    .assign(role2, entity2).id();
            relationshipType.create()
                    .assign(role1, entity2)
                    .assign(role2, entity3).id();
            relationshipType.create()
                    .assign(role1, entity2)
                    .assign(role2, entity4).id();

            List<AttributeType> attributeTypeList = new ArrayList<>();
            attributeTypeList.add(graph.putAttributeType(resourceType1, AttributeType.DataType.DOUBLE));
            attributeTypeList.add(graph.putAttributeType(resourceType2, AttributeType.DataType.LONG));
            attributeTypeList.add(graph.putAttributeType(resourceType3, AttributeType.DataType.LONG));
            attributeTypeList.add(graph.putAttributeType(resourceType4, AttributeType.DataType.STRING));
            attributeTypeList.add(graph.putAttributeType(resourceType5, AttributeType.DataType.LONG));
            attributeTypeList.add(graph.putAttributeType(resourceType6, AttributeType.DataType.DOUBLE));
            attributeTypeList.add(graph.putAttributeType(resourceType7, AttributeType.DataType.DOUBLE));

            attributeTypeList.forEach(attributeType -> {
                entityType1.has(attributeType);
                entityType2.has(attributeType);
            });

            graph.commit();
        }
    }

    private void addResourceRelations() throws InvalidKBException {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {

            Entity entity1 = graph.getConcept(entityId1);
            Entity entity2 = graph.getConcept(entityId2);
            Entity entity3 = graph.getConcept(entityId3);
            Entity entity4 = graph.getConcept(entityId4);

            entity1.has(graph.getAttributeType(resourceType1).create(1.2))
                    .has(graph.getAttributeType(resourceType1).create(1.5))
                    .has(graph.getAttributeType(resourceType2).create(4L))
                    .has(graph.getAttributeType(resourceType2).create(-1L))
                    .has(graph.getAttributeType(resourceType5).create(-7L))
                    .has(graph.getAttributeType(resourceType6).create(7.5));

            entity2.has(graph.getAttributeType(resourceType5).create(-7L))
                    .has(graph.getAttributeType(resourceType6).create(7.5));

            entity3.has(graph.getAttributeType(resourceType1).create(1.8));

            entity4.has(graph.getAttributeType(resourceType2).create(0L))
                    .has(graph.getAttributeType(resourceType5).create(-7L))
                    .has(graph.getAttributeType(resourceType6).create(7.5));

            // some resources in, but not connect them to any instances
            aDisconnectedAttribute = graph.getAttributeType(resourceType1).create(2.8).id();
            graph.getAttributeType(resourceType2).create(-5L);
            graph.getAttributeType(resourceType3).create(100L);
            graph.getAttributeType(resourceType5).create(10L);
            graph.getAttributeType(resourceType6).create(0.8);

            graph.commit();
        }
    }
}
