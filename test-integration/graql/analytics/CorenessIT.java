/*
 * Copyright (C) 2020 Grakn Labs
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

import grakn.core.concept.answer.ConceptSetMeasure;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.InvalidKBException;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static graql.lang.Graql.Token.Compute.Algorithm.K_CORE;
import static graql.lang.query.GraqlCompute.Argument.minK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class CorenessIT {
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

    @Test(expected = GraqlSemanticException.class)
    public void testKSmallerThan2_ThrowsException() {
        try (Transaction tx = session.readTransaction()) {
            tx.execute(Graql.compute().centrality().using(K_CORE).where(minK(1)));
        }
    }

    @Test
    public void testOnEmptyGraph_ReturnsEmptyMap() {
        try (Transaction tx = session.readTransaction()) {
            List<ConceptSetMeasure> result = tx.execute(Graql.compute().centrality().using(K_CORE));
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testOnGraphWithoutRelations_ReturnsEmptyMap() {
        try (Transaction tx = session.writeTransaction()) {
            tx.putEntityType(thing).create();
            tx.putEntityType(anotherThing).create();
            List<ConceptSetMeasure> result = tx.execute(Graql.compute().centrality().using(K_CORE));
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testOnGraphWithTwoEntitiesAndTwoRelations() {
        try (Transaction tx = session.writeTransaction()) {
            EntityType entityType = tx.putEntityType(thing);
            Entity entity1 = entityType.create();
            Entity entity2 = entityType.create();

            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            entityType.plays(role1).plays(role2);
            tx.putRelationType(related)
                    .relates(role1).relates(role2)
                    .create()
                    .assign(role1, entity1)
                    .assign(role2, entity2);

            Role role3 = tx.putRole("role3");
            Role role4 = tx.putRole("role4");
            entityType.plays(role3).plays(role4);
            tx.putRelationType(veryRelated)
                    .relates(role3).relates(role4)
                    .create()
                    .assign(role3, entity1)
                    .assign(role4, entity2);

            List<ConceptSetMeasure> result = tx.execute(Graql.compute().centrality().using(K_CORE));
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testOnGraphWithFourEntitiesAndSixRelations() {
        addSchemaAndEntities();

        try (Transaction tx = session.readTransaction()) {
            List<ConceptSetMeasure> result = tx.execute(Graql.compute().centrality().using(K_CORE));
            assertEquals(1, result.size());
            assertEquals(4, result.get(0).set().size());
            assertEquals(3, result.get(0).measurement().intValue());

            result = tx.execute(Graql.compute().centrality().using(K_CORE).of(thing));
            assertEquals(1, result.size());
            assertEquals(2, result.get(0).set().size());
            assertEquals(3, result.get(0).measurement().intValue());

            result = tx.execute(Graql.compute().centrality().using(K_CORE).in(thing, anotherThing, related));
            assertEquals(1, result.size());
            assertEquals(4, result.get(0).set().size());
            assertEquals(2, result.get(0).measurement().intValue());
        }
    }

    @Test
    public void testImplicitTypeShouldBeIncluded() {
        addSchemaAndEntities();

        try (Transaction tx = session.writeTransaction()) {
            String aResourceTypeLabel = "aResourceTypeLabel";
            AttributeType<String> attributeType =
                    tx.putAttributeType(aResourceTypeLabel, AttributeType.DataType.STRING);
            tx.getEntityType(thing).has(attributeType);
            tx.getEntityType(anotherThing).has(attributeType);

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

        List<ConceptSetMeasure> result;
        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().centrality().using(K_CORE));
            System.out.println("result = " + result);
            assertEquals(2, result.size());

            assertEquals(1, result.get(0).set().size());
            assertEquals(3, result.get(0).measurement().intValue());

            assertEquals(5, result.get(1).set().size());
            assertEquals(4, result.get(1).measurement().intValue());

            result = tx.execute(Graql.compute().centrality().using(K_CORE).where(minK(4L)));
            assertEquals(1, result.size());
            assertEquals(5, result.get(0).set().size());
            assertEquals(4, result.get(0).measurement().intValue());
        }
    }

    @Test
    public void testDisconnectedCores() {
        try (Transaction tx = session.writeTransaction()) {
            EntityType entityType1 = tx.putEntityType(thing);
            EntityType entityType2 = tx.putEntityType(anotherThing);

            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            RelationType relationType1 = tx.putRelationType(related)
                    .relates(role1).relates(role2);

            Role role3 = tx.putRole("role3");
            Role role4 = tx.putRole("role4");
            RelationType relationType2 = tx.putRelationType(veryRelated)
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

            relationType1.create()
                    .assign(role1, entity1)
                    .assign(role2, entity2);
            relationType1.create()
                    .assign(role1, entity2)
                    .assign(role2, entity3);
            relationType1.create()
                    .assign(role1, entity3)
                    .assign(role2, entity4);
            relationType1.create()
                    .assign(role1, entity1)
                    .assign(role2, entity3);
            relationType1.create()
                    .assign(role1, entity1)
                    .assign(role2, entity4);
            relationType1.create()
                    .assign(role1, entity2)
                    .assign(role2, entity4);

            relationType1.create()
                    .assign(role1, entity5)
                    .assign(role2, entity6);
            relationType2.create()
                    .assign(role3, entity5)
                    .assign(role4, entity7);
            relationType2.create()
                    .assign(role3, entity5)
                    .assign(role4, entity8);
            relationType2.create()
                    .assign(role3, entity6)
                    .assign(role4, entity7);
            relationType2.create()
                    .assign(role3, entity6)
                    .assign(role4, entity8);
            relationType2.create()
                    .assign(role3, entity7)
                    .assign(role4, entity8);

            relationType1.create()
                    .assign(role1, entity0)
                    .assign(role2, entity1);
            relationType1.create()
                    .assign(role1, entity0)
                    .assign(role2, entity8);

            tx.commit();
        }

        List<ConceptSetMeasure> result;
        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().centrality().using(K_CORE));
            assertEquals(2, result.size());

            assertEquals(1, result.get(0).set().size());
            assertEquals(2, result.get(0).measurement().intValue());

            assertEquals(8, result.get(1).set().size());
            assertEquals(3, result.get(1).measurement().intValue());

            result = tx.execute(Graql.compute().centrality().using(K_CORE).where(minK(3L)));
            assertEquals(1, result.size());
            assertEquals(8, result.get(0).set().size());
            assertEquals(3, result.get(0).measurement().intValue());
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

        Set<List<ConceptSetMeasure>> result = list.parallelStream().map(i -> {
            try (Transaction tx = session.readTransaction()) {
                return tx.execute(Graql.compute().centrality().using(K_CORE).where(minK(3L)));
            }
        }).collect(Collectors.toSet());
        assertEquals(1, result.size());
        result.forEach(map -> {
            assertEquals(1, map.size());
            assertEquals(4, map.get(0).set().size());
            assertEquals(3, map.get(0).measurement().intValue());
        });
    }

    private void addSchemaAndEntities() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {
            EntityType entityType1 = tx.putEntityType(thing);
            EntityType entityType2 = tx.putEntityType(anotherThing);

            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            RelationType relationType1 = tx.putRelationType(related)
                    .relates(role1).relates(role2);

            Role role3 = tx.putRole("role3");
            Role role4 = tx.putRole("role4");
            RelationType relationType2 = tx.putRelationType(veryRelated)
                    .relates(role3).relates(role4);

            entityType1.plays(role1).plays(role2).plays(role3).plays(role4);
            entityType2.plays(role1).plays(role2).plays(role3).plays(role4);

            Entity entity1 = entityType1.create();
            Entity entity2 = entityType1.create();
            Entity entity3 = entityType2.create();
            Entity entity4 = entityType2.create();

            relationType1.create()
                    .assign(role1, entity1)
                    .assign(role2, entity2);
            relationType1.create()
                    .assign(role1, entity2)
                    .assign(role2, entity3);
            relationType1.create()
                    .assign(role1, entity3)
                    .assign(role2, entity4);
            relationType1.create()
                    .assign(role1, entity4)
                    .assign(role2, entity1);

            relationType2.create()
                    .assign(role3, entity1)
                    .assign(role4, entity3);
            relationType2.create()
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