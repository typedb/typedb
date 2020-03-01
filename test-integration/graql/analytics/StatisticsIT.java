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

import com.google.common.collect.Sets;
import grakn.core.concept.answer.Numeric;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.InvalidKBException;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.exception.GraqlException;
import graql.lang.query.GraqlCompute;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class StatisticsIT {

    private static final String thing = "thingy";
    private static final String anotherThing = "anotherThing";

    private static final String resourceType1 = "resourceType1";
    private static final String resourceType2 = "resourceType2";
    private static final String resourceType3 = "resourceType3";
    private static final String resourceType4 = "resourceType4";
    private static final String resourceType5 = "resourceType5";
    private static final String resourceType6 = "resourceType6";
    private static final String resourceType7 = "resourceType7";

    private static final double delta = 0.000001;

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

    @Test
    public void testStatisticsExceptions() {
        addSchemaAndEntities();
        addResourceRelations();

        try (Transaction tx = session.readTransaction()) {
            // resources-type is not set
            assertExceptionThrown(tx, Graql.compute().max().in(thing));
            assertExceptionThrown(tx, Graql.compute().min().in(thing));
            assertExceptionThrown(tx, Graql.compute().mean().in(thing));
            assertExceptionThrown(tx, Graql.compute().sum().in(thing));
            assertExceptionThrown(tx, Graql.compute().std().in(thing));
            assertExceptionThrown(tx, Graql.compute().median().in(thing));

            // if it's not a resource-type
            assertExceptionThrown(tx, Graql.compute().max().of(thing));
            assertExceptionThrown(tx, Graql.compute().min().of(thing));
            assertExceptionThrown(tx, Graql.compute().mean().of(thing));
            assertExceptionThrown(tx, Graql.compute().sum().of(thing));
            assertExceptionThrown(tx, Graql.compute().std().of(thing));
            assertExceptionThrown(tx, Graql.compute().median().of(thing));

            // resource-type has no instance
            assertTrue(tx.execute(Graql.compute().max().of(resourceType7)).isEmpty());
            assertTrue(tx.execute(Graql.compute().min().of(resourceType7)).isEmpty());
            assertTrue(tx.execute(Graql.compute().sum().of(resourceType7)).isEmpty());
            assertTrue(tx.execute(Graql.compute().std().of(resourceType7)).isEmpty());
            assertTrue(tx.execute(Graql.compute().median().of(resourceType7)).isEmpty());
            assertTrue(tx.execute(Graql.compute().mean().of(resourceType7)).isEmpty());

            // resources are not connected to any entities
            assertTrue(tx.execute(Graql.compute().max().of(resourceType3)).isEmpty());
            assertTrue(tx.execute(Graql.compute().min().of(resourceType3)).isEmpty());
            assertTrue(tx.execute(Graql.compute().sum().of(resourceType3)).isEmpty());
            assertTrue(tx.execute(Graql.compute().std().of(resourceType3)).isEmpty());
            assertTrue(tx.execute(Graql.compute().median().of(resourceType3)).isEmpty());
            assertTrue(tx.execute(Graql.compute().mean().of(resourceType3)).isEmpty());

            // resource-type has incorrect data type
            assertExceptionThrown(tx, Graql.compute().max().of(resourceType4));
            assertExceptionThrown(tx, Graql.compute().min().of(resourceType4));
            assertExceptionThrown(tx, Graql.compute().mean().of(resourceType4));
            assertExceptionThrown(tx, Graql.compute().sum().of(resourceType4));
            assertExceptionThrown(tx, Graql.compute().std().of(resourceType4));
            assertExceptionThrown(tx, Graql.compute().median().of(resourceType4));

            // resource-types have different data types
            Set<String> resourceTypes = Sets.newHashSet(resourceType1, resourceType2);
            assertExceptionThrown(tx, Graql.compute().max().of(resourceTypes));
            assertExceptionThrown(tx, Graql.compute().min().of(resourceTypes));
            assertExceptionThrown(tx, Graql.compute().mean().of(resourceTypes));
            assertExceptionThrown(tx, Graql.compute().sum().of(resourceTypes));
            assertExceptionThrown(tx, Graql.compute().std().of(resourceTypes));
            assertExceptionThrown(tx, Graql.compute().median().of(resourceTypes));
        }
    }

    private void assertExceptionThrown(Transaction tx, GraqlCompute query) {
        boolean exceptionThrown = false;
        try {
            tx.execute(query);
        } catch (GraqlSemanticException | GraqlException e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    @Test
    public void testMinAndMax() {
        List<Numeric> result;

        // resource-type has no instance
        addSchemaAndEntities();

        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().min().of(resourceType1).in(Collections.emptyList()));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().min().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().min().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().min().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().min().of(resourceType2));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().min().of(resourceType2, resourceType5));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().min().of(resourceType2));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().min().of(resourceType2));
            assertTrue(result.isEmpty());

            result = tx.execute(Graql.compute().max().of(resourceType1).in(Collections.emptyList()));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().max().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().max().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().max().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().max().of(resourceType2).in(Collections.emptyList()));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().max().of(resourceType2, resourceType5));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().max().of(resourceType2));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().max().of(resourceType2));
            assertTrue(result.isEmpty());
        }

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().min().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().min().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().min().of(resourceType2).in(thing, anotherThing));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().min().of(resourceType2).in(anotherThing));
            assertTrue(result.isEmpty());

            result = tx.execute(Graql.compute().max().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().max().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().max().of(resourceType2).in(thing, anotherThing));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().max().of(resourceType2).in(anotherThing));
            assertTrue(result.isEmpty());
        }

        // connect entity and resources
        addResourceRelations();

        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().min().of(resourceType1).in(Collections.emptySet()));
            assertEquals(1.2, result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().min().in(thing).of(resourceType2));
            assertEquals(-1, result.get(0).number().intValue());
            result = tx.execute(Graql.compute().min().in(thing).of(resourceType2, resourceType5));
            assertEquals(-7, result.get(0).number().intValue());
            result = tx.execute(Graql.compute().min().in(thing, thing, thing).of(resourceType2, resourceType5));
            assertEquals(-7, result.get(0).number().intValue());
            result = tx.execute(Graql.compute().min().in(anotherThing).of(resourceType2));
            assertEquals(0, result.get(0).number().intValue());

            result = tx.execute(Graql.compute().max().of(resourceType1));
            assertEquals(1.8, result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().max().of(resourceType1, resourceType6));
            assertEquals(7.5, result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().max().of(resourceType1, resourceType6));
            assertEquals(7.5, result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().max().in(anotherThing).of(resourceType2));
            assertEquals(0, result.get(0).number().intValue());
        }
    }

    @Test
    public void testSum() {
        List<Numeric> result;

        // resource-type has no instance
        addSchemaAndEntities();

        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().sum().of(resourceType1).in(Collections.emptyList()));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().sum().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().sum().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().sum().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().sum().of(resourceType2));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().sum().of(resourceType2, resourceType5));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().sum().of(resourceType2));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().sum().of(resourceType2));
            assertTrue(result.isEmpty());
        }

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().sum().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().sum().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().sum().of(resourceType2).in(thing, anotherThing));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().sum().of(resourceType2).in(anotherThing));
            assertTrue(result.isEmpty());
        }

        // connect entity and resources
        addResourceRelations();

        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().sum().of(resourceType1));
            assertEquals(4.5, result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().sum().of(resourceType2).in(thing));
            assertEquals(3, result.get(0).number().intValue());
            result = tx.execute(Graql.compute().sum().of(resourceType1, resourceType6));
            assertEquals(27.0, result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().sum().of(resourceType2, resourceType5).in(thing, anotherThing));
            assertEquals(-18, result.get(0).number().intValue());
            result = tx.execute(Graql.compute().sum().of(resourceType2, resourceType5).in(thing));
            assertEquals(-11, result.get(0).number().intValue());
        }
    }

    @Test
    public void testMean() {
        List<Numeric> result;

        // resource-type has no instance
        addSchemaAndEntities();
        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().mean().of(resourceType1).in(Collections.emptyList()));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().mean().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().mean().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().mean().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().mean().of(resourceType2));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().mean().of(resourceType2, resourceType5));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().mean().of(resourceType2));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().mean().of(resourceType2));
            assertTrue(result.isEmpty());
        }

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().mean().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().mean().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().mean().of(resourceType2).in(thing, anotherThing));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().mean().of(resourceType2).in(anotherThing));
            assertTrue(result.isEmpty());
        }

        // connect entity and resources
        addResourceRelations();

        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().mean().of(resourceType1));
            assertEquals(1.5, result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().mean().of(resourceType2));
            assertEquals(1D, result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().mean().of(resourceType1, resourceType6));
            assertEquals(4.5, result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().mean().in(thing, anotherThing).of(resourceType2, resourceType5));
            assertEquals(-3D, result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().mean().in(thing).of(resourceType1, resourceType6));
            assertEquals(3.9, result.get(0).number().doubleValue(), delta);
        }
    }

    @Test
    public void testStd() {
        List<Numeric> result;

        // resource-type has no instance
        addSchemaAndEntities();

        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().std().of(resourceType1).in(Collections.emptyList()));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().std().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().std().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().std().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().std().of(resourceType2));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().std().of(resourceType2, resourceType5));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().std().of(resourceType2));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().std().of(resourceType2));
            assertTrue(result.isEmpty());
        }

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().std().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().std().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().std().of(resourceType2).in(thing, anotherThing));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().std().of(resourceType2).in(anotherThing));
            assertTrue(result.isEmpty());
        }

        // connect entity and resources
        addResourceRelations();

        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().std().of(resourceType1));
            assertEquals(Math.sqrt(0.18 / 3), result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().std().of(resourceType2).in(anotherThing));
            assertEquals(Math.sqrt(0D), result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().std().of(resourceType1, resourceType6));
            assertEquals(Math.sqrt(54.18 / 6), result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().std().of(resourceType2, resourceType5).in(thing, anotherThing));
            assertEquals(Math.sqrt(110.0 / 6), result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().std().of(resourceType2).in(thing));
            assertEquals(2.5, result.get(0).number().doubleValue(), delta);
        }

        List<Long> list = new ArrayList<>();
        long workerNumber = 3L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }

        List<Number> numberList = list.parallelStream().map(i -> {
            try (Transaction tx = session.readTransaction()) {
                return tx.execute(Graql.compute().std().of(resourceType2).in(thing)).get(0).number();
            }
        }).collect(Collectors.toList());
        numberList.forEach(value -> assertEquals(2.5D, value.doubleValue(), delta));
    }

    @Test
    public void testMedian() {
        List<Numeric> result;

        // resource-type has no instance
        addSchemaAndEntities();

        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().median().of(resourceType1).in(Collections.emptyList()));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().median().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().median().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().median().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().median().of(resourceType2));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().median().of(resourceType2, resourceType5));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().median().of(resourceType2));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().median().of(resourceType2));
            assertTrue(result.isEmpty());
        }

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().median().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().median().of(resourceType1));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().median().of(resourceType2).in(thing, anotherThing));
            assertTrue(result.isEmpty());
            result = tx.execute(Graql.compute().median().of(resourceType2).in(anotherThing));
            assertTrue(result.isEmpty());
        }

        // connect entity and resources
        addResourceRelations();

        try (Transaction tx = session.readTransaction()) {
            result = tx.execute(Graql.compute().median().of(resourceType1));
            assertEquals(1.5D, result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().median().of(resourceType6));
            assertEquals(7.5D, result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().median().of(resourceType1, resourceType6));
            assertEquals(1.8D, result.get(0).number().doubleValue(), delta);
            result = tx.execute(Graql.compute().median().of(resourceType2));
            assertEquals(0L, result.get(0).number().longValue());
            result = tx.execute(Graql.compute().median().in(thing).of(resourceType5));
            assertEquals(-7L, result.get(0).number().longValue());
            result = tx.execute(Graql.compute().median().in(thing, anotherThing).of(resourceType2, resourceType5));
            assertEquals(-7L, result.get(0).number().longValue());
            result = tx.execute(Graql.compute().median().in(thing).of(resourceType2));
            assertNotEquals(0L, result.get(0).number().longValue());
        }

        List<Long> list = new ArrayList<>();
        long workerNumber = 3L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }

        List<Number> numberList = list.parallelStream().map(i -> {
            try (Transaction tx = session.readTransaction()) {
                return tx.execute(Graql.compute().median().of(resourceType1)).get(0).number();
            }
        }).collect(Collectors.toList());
        numberList.forEach(value -> assertEquals(1.5D, value.doubleValue(), delta));
    }

    @Test
    public void testHasResourceVerticesAndEdges() {
        try (Transaction tx = session.writeTransaction()) {

            // manually construct the relation type and instance
            AttributeType<Long> power = tx.putAttributeType("power", AttributeType.DataType.LONG);
            EntityType person = tx.putEntityType("person").has(power);
            Role resourceOwner = tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of("power")).getValue());
            Role resourceValue = tx.getRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of("power")).getValue());

            person.has(power);

            Entity person1 = person.create();
            Entity person2 = person.create();
            Entity person3 = person.create();
            Attribute power1 = power.create(1L);
            Attribute power2 = power.create(2L);
            Attribute power3 = power.create(3L);
            RelationType relationType = tx.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of("power")))
                    .relates(resourceOwner).relates(resourceValue);

            relationType.create()
                    .assign(resourceOwner, person1)
                    .assign(resourceValue, power1);

            relationType.create()
                    .assign(resourceOwner, person2)
                    .assign(resourceValue, power2);
            person1.has(power2);

            person3.has(power3);

            tx.commit();
        }

        Numeric result;

        try (Transaction tx = session.readTransaction()) {
            // No need to test all statistics as most of them share the same vertex program

            result = tx.execute(Graql.compute().min().of("power")).get(0);
            assertEquals(1L, result.number().longValue());

            result = tx.execute(Graql.compute().max().of("power")).get(0);
            assertEquals(3L, result.number().longValue());

            result = tx.execute(Graql.compute().sum().of("power")).get(0);
            assertEquals(8L, result.number().longValue());

            result = tx.execute(Graql.compute().median().of("power")).get(0);
            assertEquals(2L, result.number().longValue());
        }
    }

    private void addSchemaAndEntities() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {
            EntityType entityType1 = tx.putEntityType(thing);
            EntityType entityType2 = tx.putEntityType(anotherThing);

            Entity entity1 = entityType1.create();
            Entity entity2 = entityType1.create();
            Entity entity3 = entityType1.create();
            Entity entity4 = entityType2.create();
            entityId1 = entity1.id();
            entityId2 = entity2.id();
            entityId3 = entity3.id();
            entityId4 = entity4.id();

            Role relation1 = tx.putRole("relation1");
            Role relation2 = tx.putRole("relation2");
            entityType1.plays(relation1).plays(relation2);
            entityType2.plays(relation1).plays(relation2);
            RelationType related = tx.putRelationType("related").relates(relation1).relates(relation2);

            related.create()
                    .assign(relation1, entity1)
                    .assign(relation2, entity2);
            related.create()
                    .assign(relation1, entity2)
                    .assign(relation2, entity3);
            related.create()
                    .assign(relation1, entity2)
                    .assign(relation2, entity4);

            AttributeType<Double> attribute1 = tx.putAttributeType(resourceType1, AttributeType.DataType.DOUBLE);
            AttributeType<Long> attribute2 = tx.putAttributeType(resourceType2, AttributeType.DataType.LONG);
            AttributeType<Long> attribute3 = tx.putAttributeType(resourceType3, AttributeType.DataType.LONG);
            AttributeType<String> attribute4 = tx.putAttributeType(resourceType4, AttributeType.DataType.STRING);
            AttributeType<Long> attribute5 = tx.putAttributeType(resourceType5, AttributeType.DataType.LONG);
            AttributeType<Double> attribute6 = tx.putAttributeType(resourceType6, AttributeType.DataType.DOUBLE);
            AttributeType<Double> attribute7 = tx.putAttributeType(resourceType7, AttributeType.DataType.DOUBLE);

            entityType1.has(attribute1);
            entityType1.has(attribute2);
            entityType1.has(attribute3);
            entityType1.has(attribute4);
            entityType1.has(attribute5);
            entityType1.has(attribute6);
            entityType1.has(attribute7);

            entityType2.has(attribute1);
            entityType2.has(attribute2);
            entityType2.has(attribute3);
            entityType2.has(attribute4);
            entityType2.has(attribute5);
            entityType2.has(attribute6);
            entityType2.has(attribute7);

            tx.commit();
        }
    }

    private void addResourcesInstances() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {
            tx.<Double>getAttributeType(resourceType1).create(1.2);
            tx.<Double>getAttributeType(resourceType1).create(1.5);
            tx.<Double>getAttributeType(resourceType1).create(1.8);

            tx.<Long>getAttributeType(resourceType2).create(4L);
            tx.<Long>getAttributeType(resourceType2).create(-1L);
            tx.<Long>getAttributeType(resourceType2).create(0L);

            tx.<Long>getAttributeType(resourceType5).create(6L);
            tx.<Long>getAttributeType(resourceType5).create(7L);
            tx.<Long>getAttributeType(resourceType5).create(8L);

            tx.<Double>getAttributeType(resourceType6).create(7.2);
            tx.<Double>getAttributeType(resourceType6).create(7.5);
            tx.<Double>getAttributeType(resourceType6).create(7.8);

            tx.<String>getAttributeType(resourceType4).create("a");
            tx.<String>getAttributeType(resourceType4).create("b");
            tx.<String>getAttributeType(resourceType4).create("c");

            tx.commit();
        }
    }

    private void addResourceRelations() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {
            Entity entity1 = tx.getConcept(entityId1);
            Entity entity2 = tx.getConcept(entityId2);
            Entity entity3 = tx.getConcept(entityId3);
            Entity entity4 = tx.getConcept(entityId4);

            Role resourceOwner1 = tx.getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType1)));
            Role resourceOwner2 = tx.getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType2)));
            Role resourceOwner3 = tx.getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType3)));
            Role resourceOwner4 = tx.getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType4)));
            Role resourceOwner5 = tx.getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType5)));
            Role resourceOwner6 = tx.getSchemaConcept(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType6)));

            Role resourceValue1 = tx.getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType1)));
            Role resourceValue2 = tx.getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType2)));
            Role resourceValue3 = tx.getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType3)));
            Role resourceValue4 = tx.getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType4)));
            Role resourceValue5 = tx.getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType5)));
            Role resourceValue6 = tx.getSchemaConcept(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType6)));

            RelationType relationType1 = tx.getSchemaConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType1)));
            relationType1.create()
                    .assign(resourceOwner1, entity1)
                    .assign(resourceValue1, tx.<Double>getAttributeType(resourceType1).create(1.2));
            relationType1.create()
                    .assign(resourceOwner1, entity1)
                    .assign(resourceValue1, tx.<Double>getAttributeType(resourceType1).create(1.5));
            relationType1.create()
                    .assign(resourceOwner1, entity3)
                    .assign(resourceValue1, tx.<Double>getAttributeType(resourceType1).create(1.8));

            RelationType relationType2 = tx.getSchemaConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType2)));
            relationType2.create()
                    .assign(resourceOwner2, entity1)
                    .assign(resourceValue2, tx.<Long>getAttributeType(resourceType2).create(4L));
            relationType2.create()
                    .assign(resourceOwner2, entity1)
                    .assign(resourceValue2, tx.<Long>getAttributeType(resourceType2).create(-1L));
            relationType2.create()
                    .assign(resourceOwner2, entity4)
                    .assign(resourceValue2, tx.<Long>getAttributeType(resourceType2).create(0L));

            tx.<Long>getAttributeType(resourceType3).create(100L);

            RelationType relationType5 = tx.getSchemaConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType5)));
            relationType5.create()
                    .assign(resourceOwner5, entity1)
                    .assign(resourceValue5, tx.<Long>getAttributeType(resourceType5).create(-7L));
            relationType5.create()
                    .assign(resourceOwner5, entity2)
                    .assign(resourceValue5, tx.<Long>getAttributeType(resourceType5).create(-7L));
            relationType5.create()
                    .assign(resourceOwner5, entity4)
                    .assign(resourceValue5, tx.<Long>getAttributeType(resourceType5).create(-7L));

            RelationType relationType6 = tx.getSchemaConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType6)));
            relationType6.create()
                    .assign(resourceOwner6, entity1)
                    .assign(resourceValue6, tx.<Double>getAttributeType(resourceType6).create(7.5));
            relationType6.create()
                    .assign(resourceOwner6, entity2)
                    .assign(resourceValue6, tx.<Double>getAttributeType(resourceType6).create(7.5));
            relationType6.create()
                    .assign(resourceOwner6, entity4)
                    .assign(resourceValue6, tx.<Double>getAttributeType(resourceType6).create(7.5));

            // some resources in, but not connect them to any instances
            tx.<Double>getAttributeType(resourceType1).create(2.8);
            tx.<Long>getAttributeType(resourceType2).create(-5L);
            tx.<Long>getAttributeType(resourceType5).create(10L);
            tx.<Double>getAttributeType(resourceType6).create(0.8);

            tx.commit();
        }
    }
}