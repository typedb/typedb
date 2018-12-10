/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.analytics;

import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Entity;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.server.exception.InvalidKBException;
import grakn.core.graql.query.Graql;
import grakn.core.graql.answer.ConceptSet;
import grakn.core.rule.GraknTestServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.graql.query.ComputeQuery.Algorithm.K_CORE;
import static grakn.core.graql.query.ComputeQuery.Argument.k;
import static grakn.core.graql.query.ComputeQuery.Method.CLUSTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class KCoreIT {
    private static final String thing = "thingy";
    private static final String anotherThing = "anotherThing";
    private static final String related = "related";
    private static final String veryRelated = "veryRelated";

    private ConceptId entityId1;
    private ConceptId entityId2;
    private ConceptId entityId3;
    private ConceptId entityId4;

    public Session session;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
    }

    @After
    public void closeSession() { session.close(); }

    @Test(expected = GraqlQueryException.class)
    public void testKSmallerThan2_ThrowsException() {
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            tx.graql().compute(CLUSTER).using(K_CORE).where(k(1L)).execute();
        }
    }

    @Test
    public void testOnEmptyGraph_ReturnsEmptyMap() {
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            List<ConceptSet> result = tx.graql().compute(CLUSTER).using(K_CORE).where(k(2L)).execute();
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testOnGraphWithoutRelationships_ReturnsEmptyMap() {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType(thing).create();
            tx.putEntityType(anotherThing).create();
            List<ConceptSet> result = tx.graql().compute(CLUSTER).using(K_CORE).where(k(2L)).execute();
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testOnGraphWithTwoEntitiesAndTwoRelationships() {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            EntityType entityType = tx.putEntityType(thing);
            Entity entity1 = entityType.create();
            Entity entity2 = entityType.create();

            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            entityType.plays(role1).plays(role2);
            tx.putRelationshipType(related)
                    .relates(role1).relates(role2)
                    .create()
                    .assign(role1, entity1)
                    .assign(role2, entity2);

            Role role3 = tx.putRole("role3");
            Role role4 = tx.putRole("role4");
            entityType.plays(role3).plays(role4);
            tx.putRelationshipType(veryRelated)
                    .relates(role3).relates(role4)
                    .create()
                    .assign(role3, entity1)
                    .assign(role4, entity2);

            List<ConceptSet> result = tx.graql().compute(CLUSTER).using(K_CORE).where(k(2L)).execute();
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testOnGraphWithFourEntitiesAndSixRelationships() {
        addSchemaAndEntities();

        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            List<ConceptSet> result1 = tx.graql().compute(CLUSTER).using(K_CORE).where(k(2L)).execute();
            assertEquals(1, result1.size());
            assertEquals(4, result1.iterator().next().set().size());

            List<ConceptSet> result2 = tx.graql().compute(CLUSTER).using(K_CORE).where(k(3L)).execute();
            assertEquals(result1, result2);

            result2 = tx.graql().compute(CLUSTER).using(K_CORE).where(k(2L)).in(thing, related).execute();
            assertEquals(result1, result2);

            result2 = tx.graql().compute(CLUSTER).using(K_CORE).where(k(3L)).in(thing, related).execute();
            assertTrue(result2.isEmpty());
        }
    }

    @Test
    public void testImplicitTypeShouldBeExcluded() {
        addSchemaAndEntities();

        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            String aResourceTypeLabel = "aResourceTypeLabel";
            AttributeType<String> attributeType =
                    tx.putAttributeType(aResourceTypeLabel, AttributeType.DataType.STRING);
            tx.getEntityType(thing).has(attributeType);
            Attribute aAttribute = attributeType.create("blah");
            tx.getConcept(entityId1).asEntity().has(aAttribute);
            tx.getConcept(entityId2).asEntity().has(aAttribute);

            tx.commit();
        }

        List<ConceptSet> result;
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            result = tx.graql().compute(CLUSTER).using(K_CORE).includeAttributes(true).where(k(2L)).execute();
            assertEquals(1, result.size());
            assertEquals(5, result.iterator().next().set().size());

            result = tx.graql().compute(CLUSTER).using(K_CORE).where(k(3L)).execute();
            assertEquals(1, result.size());
            assertEquals(4, result.iterator().next().set().size());
        }
    }

    @Test
    public void testImplicitTypeShouldBeIncluded() {
        addSchemaAndEntities();

        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            String aResourceTypeLabel = "aResourceTypeLabel";
            AttributeType<String> attributeType =
                    tx.putAttributeType(aResourceTypeLabel, AttributeType.DataType.STRING);
            tx.getEntityType(thing).has(attributeType);

            Attribute Attribute1 = attributeType.create("blah");
            tx.getConcept(entityId1).asEntity().has(Attribute1);
            tx.getConcept(entityId2).asEntity().has(Attribute1);
            tx.getConcept(entityId3).asEntity().has(Attribute1);
            tx.getConcept(entityId4).asEntity().has(Attribute1);

            Attribute Attribute2 = attributeType.create("bah");
            tx.getConcept(entityId1).asEntity().has(Attribute2);
            tx.getConcept(entityId2).asEntity().has(Attribute2);
            tx.getConcept(entityId3).asEntity().has(Attribute2);

            tx.commit();
        }

        List<ConceptSet> result;
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            result = tx.graql().compute(CLUSTER).using(K_CORE).includeAttributes(true).where(k(4L)).execute();
            System.out.println("result = " + result);
            assertEquals(1, result.size());
            assertEquals(5, result.iterator().next().set().size());

            result = tx.graql().compute(CLUSTER).using(K_CORE).where(k(3L)).includeAttributes(true).execute();
            System.out.println("result = " + result);
            assertEquals(1, result.size());
            assertEquals(6, result.iterator().next().set().size());
        }
    }

    @Test
    public void testDisconnectedCores() {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            EntityType entityType1 = tx.putEntityType(thing);
            EntityType entityType2 = tx.putEntityType(anotherThing);

            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            RelationshipType relationshipType1 = tx.putRelationshipType(related)
                    .relates(role1).relates(role2);

            Role role3 = tx.putRole("role3");
            Role role4 = tx.putRole("role4");
            RelationshipType relationshipType2 = tx.putRelationshipType(veryRelated)
                    .relates(role3).relates(role4);

            entityType1.plays(role1).plays(role2).plays(role3).plays(role4);
            entityType2.plays(role1).plays(role2).plays(role3).plays(role4);

            Entity entity0 = entityType1.create();
            Entity entity1 = entityType1.create();
            Entity entity2 = entityType1.create();
            Entity entity3 = entityType1.create();
            Entity entity4 = entityType1.create();
            Entity entity5 = entityType1.create();
            Entity entity6 = entityType1.create();
            Entity entity7 = entityType1.create();
            Entity entity8 = entityType1.create();

            relationshipType1.create()
                    .assign(role1, entity1)
                    .assign(role2, entity2);
            relationshipType1.create()
                    .assign(role1, entity2)
                    .assign(role2, entity3);
            relationshipType1.create()
                    .assign(role1, entity3)
                    .assign(role2, entity4);
            relationshipType1.create()
                    .assign(role1, entity1)
                    .assign(role2, entity3);
            relationshipType1.create()
                    .assign(role1, entity1)
                    .assign(role2, entity4);
            relationshipType1.create()
                    .assign(role1, entity2)
                    .assign(role2, entity4);

            relationshipType1.create()
                    .assign(role1, entity5)
                    .assign(role2, entity6);
            relationshipType2.create()
                    .assign(role3, entity5)
                    .assign(role4, entity7);
            relationshipType2.create()
                    .assign(role3, entity5)
                    .assign(role4, entity8);
            relationshipType2.create()
                    .assign(role3, entity6)
                    .assign(role4, entity7);
            relationshipType2.create()
                    .assign(role3, entity6)
                    .assign(role4, entity8);
            relationshipType2.create()
                    .assign(role3, entity7)
                    .assign(role4, entity8);

            relationshipType1.create()
                    .assign(role1, entity0)
                    .assign(role2, entity1);
            relationshipType1.create()
                    .assign(role1, entity0)
                    .assign(role2, entity8);

            tx.commit();
        }

        List<ConceptSet> result;
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            result = tx.graql().compute(CLUSTER).using(K_CORE).where(k(3L)).execute();
            assertEquals(2, result.size());
            assertEquals(4, result.iterator().next().set().size());

            System.out.println("result = " + result);
            result = tx.graql().compute(CLUSTER).using(K_CORE).where(k(2L)).execute();
            assertEquals(1, result.size());
            assertEquals(9, result.iterator().next().set().size());
        }
    }

    @Test
    public void testConcurrency() {
        addSchemaAndEntities();

        List<Long> list = new ArrayList<>(4);
        long workerNumber = 4L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }

        Set<List<ConceptSet>> result = list.parallelStream().map(i -> {
            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
                return tx.graql().compute(CLUSTER).using(K_CORE).where(k(3L)).execute();
            }
        }).collect(Collectors.toSet());
        result.forEach(map -> {
            assertEquals(1, map.size());
            assertEquals(4, map.iterator().next().set().size());
        });
    }

    private void addSchemaAndEntities() throws InvalidKBException {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            EntityType entityType1 = tx.putEntityType(thing);
            EntityType entityType2 = tx.putEntityType(anotherThing);

            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            RelationshipType relationshipType1 = tx.putRelationshipType(related)
                    .relates(role1).relates(role2);

            Role role3 = tx.putRole("role3");
            Role role4 = tx.putRole("role4");
            RelationshipType relationshipType2 = tx.putRelationshipType(veryRelated)
                    .relates(role3).relates(role4);

            entityType1.plays(role1).plays(role2).plays(role3).plays(role4);
            entityType2.plays(role1).plays(role2).plays(role3).plays(role4);

            Entity entity1 = entityType1.create();
            Entity entity2 = entityType1.create();
            Entity entity3 = entityType1.create();
            Entity entity4 = entityType1.create();

            relationshipType1.create()
                    .assign(role1, entity1)
                    .assign(role2, entity2);
            relationshipType1.create()
                    .assign(role1, entity2)
                    .assign(role2, entity3);
            relationshipType1.create()
                    .assign(role1, entity3)
                    .assign(role2, entity4);
            relationshipType1.create()
                    .assign(role1, entity4)
                    .assign(role2, entity1);

            relationshipType2.create()
                    .assign(role3, entity1)
                    .assign(role4, entity3);
            relationshipType2.create()
                    .assign(role3, entity2)
                    .assign(role4, entity4);

            entityId1 = entity1.id();
            entityId2 = entity2.id();
            entityId3 = entity3.id();
            entityId4 = entity4.id();

            tx.commit();
        }
    }
}