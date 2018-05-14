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
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.Graql;
import ai.grakn.test.rule.SessionContext;
import ai.grakn.util.GraknTestUtil;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.K_CORE;
import static ai.grakn.util.GraqlSyntax.Compute.Argument.k;
import static ai.grakn.util.GraqlSyntax.Compute.Method.CLUSTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class KCoreTest {
    private static final String thing = "thingy";
    private static final String anotherThing = "anotherThing";
    private static final String related = "related";
    private static final String veryRelated = "veryRelated";

    private ConceptId entityId1;
    private ConceptId entityId2;
    private ConceptId entityId3;
    private ConceptId entityId4;

    public GraknSession session;

    @ClassRule
    public final static SessionContext sessionContext = SessionContext.create();

    @Before
    public void setUp() {
        session = sessionContext.newSession();
    }

    @Test(expected = GraqlQueryException.class)
    public void testKSmallerThan2_ThrowsException() {
        try (GraknTx graph = session.open(GraknTxType.READ)) {
            graph.graql().compute(CLUSTER).using(K_CORE).where(k(1L)).execute();
        }
    }

    @Test
    public void testOnEmptyGraph_ReturnsEmptyMap() {
        try (GraknTx graph = session.open(GraknTxType.READ)) {
            List<Set<ConceptId>> result = graph.graql().compute(CLUSTER).using(K_CORE).where(k(2L)).execute().getClusterMembers().get();
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testOnGraphWithoutRelationships_ReturnsEmptyMap() {
        try (GraknTx graph = session.open(GraknTxType.WRITE)) {
            graph.putEntityType(thing).addEntity();
            graph.putEntityType(anotherThing).addEntity();
            List<Set<ConceptId>> result = graph.graql().compute(CLUSTER).using(K_CORE).where(k(2L)).execute().getClusterMembers().get();
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testOnGraphWithTwoEntitiesAndTwoRelationships() {
        try (GraknTx graph = session.open(GraknTxType.WRITE)) {
            EntityType entityType = graph.putEntityType(thing);
            Entity entity1 = entityType.addEntity();
            Entity entity2 = entityType.addEntity();

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
            entityType.plays(role1).plays(role2);
            graph.putRelationshipType(related)
                    .relates(role1).relates(role2)
                    .addRelationship()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity2);

            Role role3 = graph.putRole("role3");
            Role role4 = graph.putRole("role4");
            entityType.plays(role3).plays(role4);
            graph.putRelationshipType(veryRelated)
                    .relates(role3).relates(role4)
                    .addRelationship()
                    .addRolePlayer(role3, entity1)
                    .addRolePlayer(role4, entity2);

            List<Set<ConceptId>> result = graph.graql().compute(CLUSTER).using(K_CORE).where(k(2L)).execute().getClusterMembers().get();
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testOnGraphWithFourEntitiesAndSixRelationships() {
        addSchemaAndEntities();

        try (GraknTx graph = session.open(GraknTxType.READ)) {
            List<Set<ConceptId>> result1 = graph.graql().compute(CLUSTER).using(K_CORE).where(k(2L)).execute().getClusterMembers().get();
            assertEquals(1, result1.size());
            assertEquals(4, result1.iterator().next().size());

            List<Set<ConceptId>> result2 = graph.graql().compute(CLUSTER).using(K_CORE).where(k(3L)).execute().getClusterMembers().get();
            assertEquals(result1, result2);

            result2 = graph.graql().compute(CLUSTER).using(K_CORE).where(k(2L)).in(thing, related).execute().getClusterMembers().get();
            assertEquals(result1, result2);

            result2 = graph.graql().compute(CLUSTER).using(K_CORE).where(k(3L)).in(thing, related).execute().getClusterMembers().get();
            assertTrue(result2.isEmpty());
        }
    }

    @Test
    public void testImplicitTypeShouldBeExcluded() {
        addSchemaAndEntities();

        try (GraknTx graph = session.open(GraknTxType.WRITE)) {
            String aResourceTypeLabel = "aResourceTypeLabel";
            AttributeType<String> attributeType =
                    graph.putAttributeType(aResourceTypeLabel, AttributeType.DataType.STRING);
            graph.getEntityType(thing).attribute(attributeType);
            Attribute aAttribute = attributeType.putAttribute("blah");
            graph.getConcept(entityId1).asEntity().attribute(aAttribute);
            graph.getConcept(entityId2).asEntity().attribute(aAttribute);

            graph.commit();
        }

        List<Set<ConceptId>> result;
        try (GraknTx graph = session.open(GraknTxType.READ)) {
            result = graph.graql().compute(CLUSTER).using(K_CORE).includeAttributes(true).where(k(2L)).execute().getClusterMembers().get();
            assertEquals(1, result.size());
            assertEquals(5, result.iterator().next().size());

            result = graph.graql().compute(CLUSTER).using(K_CORE).where(k(3L)).execute().getClusterMembers().get();
            assertEquals(1, result.size());
            assertEquals(4, result.iterator().next().size());
        }
    }

    @Test
    public void testImplicitTypeShouldBeIncluded() {
        addSchemaAndEntities();

        try (GraknTx graph = session.open(GraknTxType.WRITE)) {
            String aResourceTypeLabel = "aResourceTypeLabel";
            AttributeType<String> attributeType =
                    graph.putAttributeType(aResourceTypeLabel, AttributeType.DataType.STRING);
            graph.getEntityType(thing).attribute(attributeType);

            Attribute Attribute1 = attributeType.putAttribute("blah");
            graph.getConcept(entityId1).asEntity().attribute(Attribute1);
            graph.getConcept(entityId2).asEntity().attribute(Attribute1);
            graph.getConcept(entityId3).asEntity().attribute(Attribute1);
            graph.getConcept(entityId4).asEntity().attribute(Attribute1);

            Attribute Attribute2 = attributeType.putAttribute("bah");
            graph.getConcept(entityId1).asEntity().attribute(Attribute2);
            graph.getConcept(entityId2).asEntity().attribute(Attribute2);
            graph.getConcept(entityId3).asEntity().attribute(Attribute2);

            graph.commit();
        }

        List<Set<ConceptId>> result;
        try (GraknTx graph = session.open(GraknTxType.READ)) {
            result = graph.graql().compute(CLUSTER).using(K_CORE).includeAttributes(true).where(k(4L)).execute().getClusterMembers().get();
            System.out.println("result = " + result);
            assertEquals(1, result.size());
            assertEquals(5, result.iterator().next().size());

            result = graph.graql().compute(CLUSTER).using(K_CORE).where(k(3L)).includeAttributes(true).execute().getClusterMembers().get();
            System.out.println("result = " + result);
            assertEquals(1, result.size());
            assertEquals(6, result.iterator().next().size());
        }
    }

    @Test
    public void testDisconnectedCores() {
        try (GraknTx graph = session.open(GraknTxType.WRITE)) {
            EntityType entityType1 = graph.putEntityType(thing);
            EntityType entityType2 = graph.putEntityType(anotherThing);

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
            RelationshipType relationshipType1 = graph.putRelationshipType(related)
                    .relates(role1).relates(role2);

            Role role3 = graph.putRole("role3");
            Role role4 = graph.putRole("role4");
            RelationshipType relationshipType2 = graph.putRelationshipType(veryRelated)
                    .relates(role3).relates(role4);

            entityType1.plays(role1).plays(role2).plays(role3).plays(role4);
            entityType2.plays(role1).plays(role2).plays(role3).plays(role4);

            Entity entity0 = entityType1.addEntity();
            Entity entity1 = entityType1.addEntity();
            Entity entity2 = entityType1.addEntity();
            Entity entity3 = entityType1.addEntity();
            Entity entity4 = entityType1.addEntity();
            Entity entity5 = entityType1.addEntity();
            Entity entity6 = entityType1.addEntity();
            Entity entity7 = entityType1.addEntity();
            Entity entity8 = entityType1.addEntity();

            relationshipType1.addRelationship()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity2);
            relationshipType1.addRelationship()
                    .addRolePlayer(role1, entity2)
                    .addRolePlayer(role2, entity3);
            relationshipType1.addRelationship()
                    .addRolePlayer(role1, entity3)
                    .addRolePlayer(role2, entity4);
            relationshipType1.addRelationship()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity3);
            relationshipType1.addRelationship()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity4);
            relationshipType1.addRelationship()
                    .addRolePlayer(role1, entity2)
                    .addRolePlayer(role2, entity4);

            relationshipType1.addRelationship()
                    .addRolePlayer(role1, entity5)
                    .addRolePlayer(role2, entity6);
            relationshipType2.addRelationship()
                    .addRolePlayer(role3, entity5)
                    .addRolePlayer(role4, entity7);
            relationshipType2.addRelationship()
                    .addRolePlayer(role3, entity5)
                    .addRolePlayer(role4, entity8);
            relationshipType2.addRelationship()
                    .addRolePlayer(role3, entity6)
                    .addRolePlayer(role4, entity7);
            relationshipType2.addRelationship()
                    .addRolePlayer(role3, entity6)
                    .addRolePlayer(role4, entity8);
            relationshipType2.addRelationship()
                    .addRolePlayer(role3, entity7)
                    .addRolePlayer(role4, entity8);

            relationshipType1.addRelationship()
                    .addRolePlayer(role1, entity0)
                    .addRolePlayer(role2, entity1);
            relationshipType1.addRelationship()
                    .addRolePlayer(role1, entity0)
                    .addRolePlayer(role2, entity8);

            graph.commit();
        }

        List<Set<ConceptId>> result;
        try (GraknTx graph = session.open(GraknTxType.READ)) {
            result = graph.graql().compute(CLUSTER).using(K_CORE).where(k(3L)).execute().getClusterMembers().get();
            assertEquals(2, result.size());
            assertEquals(4, result.iterator().next().size());

            System.out.println("result = " + result);
            result = graph.graql().compute(CLUSTER).using(K_CORE).where(k(2L)).execute().getClusterMembers().get();
            assertEquals(1, result.size());
            assertEquals(9, result.iterator().next().size());
        }
    }

    @Test
    public void testConcurrency() {
        assumeFalse(GraknTestUtil.usingTinker());

        addSchemaAndEntities();

        List<Long> list = new ArrayList<>(4);
        long workerNumber = 4L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }

        Set<List<Set<ConceptId>>> result = list.parallelStream().map(i -> {
            try (GraknTx graph = session.open(GraknTxType.READ)) {
                return Graql.compute(CLUSTER).withTx(graph).using(K_CORE).where(k(3L)).execute().getClusterMembers().get();
            }
        }).collect(Collectors.toSet());
        result.forEach(map -> {
            assertEquals(1, map.size());
            assertEquals(4, map.iterator().next().size());
        });
    }

    private void addSchemaAndEntities() throws InvalidKBException {
        try (GraknTx graph = session.open(GraknTxType.WRITE)) {
            EntityType entityType1 = graph.putEntityType(thing);
            EntityType entityType2 = graph.putEntityType(anotherThing);

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
            RelationshipType relationshipType1 = graph.putRelationshipType(related)
                    .relates(role1).relates(role2);

            Role role3 = graph.putRole("role3");
            Role role4 = graph.putRole("role4");
            RelationshipType relationshipType2 = graph.putRelationshipType(veryRelated)
                    .relates(role3).relates(role4);

            entityType1.plays(role1).plays(role2).plays(role3).plays(role4);
            entityType2.plays(role1).plays(role2).plays(role3).plays(role4);

            Entity entity1 = entityType1.addEntity();
            Entity entity2 = entityType1.addEntity();
            Entity entity3 = entityType1.addEntity();
            Entity entity4 = entityType1.addEntity();

            relationshipType1.addRelationship()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity2);
            relationshipType1.addRelationship()
                    .addRolePlayer(role1, entity2)
                    .addRolePlayer(role2, entity3);
            relationshipType1.addRelationship()
                    .addRolePlayer(role1, entity3)
                    .addRolePlayer(role2, entity4);
            relationshipType1.addRelationship()
                    .addRolePlayer(role1, entity4)
                    .addRolePlayer(role2, entity1);

            relationshipType2.addRelationship()
                    .addRolePlayer(role3, entity1)
                    .addRolePlayer(role4, entity3);
            relationshipType2.addRelationship()
                    .addRolePlayer(role3, entity2)
                    .addRolePlayer(role4, entity4);

            entityId1 = entity1.getId();
            entityId2 = entity2.getId();
            entityId3 = entity3.getId();
            entityId4 = entity4.getId();

            graph.commit();
        }
    }
}
