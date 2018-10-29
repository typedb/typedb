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
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.answer.Value;
import ai.grakn.test.rule.EmbeddedCassandraContext;
import ai.grakn.test.rule.ServerContext;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.util.GraqlSyntax.Compute.Method.MAX;
import static ai.grakn.util.GraqlSyntax.Compute.Method.MEAN;
import static ai.grakn.util.GraqlSyntax.Compute.Method.MEDIAN;
import static ai.grakn.util.GraqlSyntax.Compute.Method.MIN;
import static ai.grakn.util.GraqlSyntax.Compute.Method.STD;
import static ai.grakn.util.GraqlSyntax.Compute.Method.SUM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
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

    public GraknSession session;

    private static EmbeddedCassandraContext cassandraContext = new EmbeddedCassandraContext();
    private static final ServerContext server = new ServerContext();

    @ClassRule
    public static RuleChain chain = RuleChain.outerRule(cassandraContext).around(server);

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
    }

    @Test
    public void testStatisticsExceptions() {
        addSchemaAndEntities();
        addResourceRelations();

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            // resources-type is not set
            assertGraqlQueryExceptionThrown(tx.graql().compute(MAX).in(thing));
            assertGraqlQueryExceptionThrown(tx.graql().compute(MIN).in(thing));
            assertGraqlQueryExceptionThrown(tx.graql().compute(MEAN).in(thing));
            assertGraqlQueryExceptionThrown(tx.graql().compute(SUM).in(thing));
            assertGraqlQueryExceptionThrown(tx.graql().compute(STD).in(thing));
            assertGraqlQueryExceptionThrown(tx.graql().compute(MEDIAN).in(thing));

            // if it's not a resource-type
            assertGraqlQueryExceptionThrown(tx.graql().compute(MAX).of(thing));
            assertGraqlQueryExceptionThrown(tx.graql().compute(MIN).of(thing));
            assertGraqlQueryExceptionThrown(tx.graql().compute(MEAN).of(thing));
            assertGraqlQueryExceptionThrown(tx.graql().compute(SUM).of(thing));
            assertGraqlQueryExceptionThrown(tx.graql().compute(STD).of(thing));
            assertGraqlQueryExceptionThrown(tx.graql().compute(MEDIAN).of(thing));

            // resource-type has no instance
            assertTrue(tx.graql().compute(MAX).of(resourceType7).execute().isEmpty());
            assertTrue(tx.graql().compute(MIN).of(resourceType7).execute().isEmpty());
            assertTrue(tx.graql().compute(SUM).of(resourceType7).execute().isEmpty());
            assertTrue(tx.graql().compute(STD).of(resourceType7).execute().isEmpty());
            assertTrue(tx.graql().compute(MEDIAN).of(resourceType7).execute().isEmpty());
            assertTrue(tx.graql().compute(MEAN).of(resourceType7).execute().isEmpty());

            // resources are not connected to any entities
            assertTrue(tx.graql().compute(MAX).of(resourceType3).execute().isEmpty());
            assertTrue(tx.graql().compute(MIN).of(resourceType3).execute().isEmpty());
            assertTrue(tx.graql().compute(SUM).of(resourceType3).execute().isEmpty());
            assertTrue(tx.graql().compute(STD).of(resourceType3).execute().isEmpty());
            assertTrue(tx.graql().compute(MEDIAN).of(resourceType3).execute().isEmpty());
            assertTrue(tx.graql().compute(MEAN).of(resourceType3).execute().isEmpty());

            // resource-type has incorrect data type
            assertGraqlQueryExceptionThrown(tx.graql().compute(MAX).of(resourceType4));
            assertGraqlQueryExceptionThrown(tx.graql().compute(MIN).of(resourceType4));
            assertGraqlQueryExceptionThrown(tx.graql().compute(MEAN).of(resourceType4));
            assertGraqlQueryExceptionThrown(tx.graql().compute(SUM).of(resourceType4));
            assertGraqlQueryExceptionThrown(tx.graql().compute(STD).of(resourceType4));
            assertGraqlQueryExceptionThrown(tx.graql().compute(MEDIAN).of(resourceType4));

            // resource-types have different data types
            Set<Label> resourceTypes = Sets.newHashSet(Label.of(resourceType1), Label.of(resourceType2));
            assertGraqlQueryExceptionThrown(tx.graql().compute(MAX).of(resourceTypes));
            assertGraqlQueryExceptionThrown(tx.graql().compute(MIN).of(resourceTypes));
            assertGraqlQueryExceptionThrown(tx.graql().compute(MEAN).of(resourceTypes));
            assertGraqlQueryExceptionThrown(tx.graql().compute(SUM).of(resourceTypes));
            assertGraqlQueryExceptionThrown(tx.graql().compute(STD).of(resourceTypes));
            assertGraqlQueryExceptionThrown(tx.graql().compute(MEDIAN).of(resourceTypes));
        }
    }

    private void assertGraqlQueryExceptionThrown(ComputeQuery query) {
        boolean exceptionThrown = false;
        try {
            query.execute();
        } catch (GraqlQueryException e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    @Test
    public void testMinAndMax() {
        List<Value> result;

        // resource-type has no instance
        addSchemaAndEntities();

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            result = Graql.compute(MIN).of(resourceType1).in(Collections.emptyList()).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MIN).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MIN).withTx(tx).of(resourceType1).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MIN).withTx(tx).of(resourceType1).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MIN).of(resourceType2).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MIN).of(resourceType2, resourceType5).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MIN).of(resourceType2).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MIN).withTx(tx).of(resourceType2).execute();
            assertTrue(result.isEmpty());

            result = Graql.compute(MAX).of(resourceType1).in(Collections.emptyList()).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MAX).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MAX).withTx(tx).of(resourceType1).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MAX).withTx(tx).of(resourceType1).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MAX).of(resourceType2).in(Collections.emptyList()).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MAX).of(resourceType2, resourceType5).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MAX).of(resourceType2).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MAX).withTx(tx).of(resourceType2).execute();
            assertTrue(result.isEmpty());
        }

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            result = Graql.compute(MIN).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MIN).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MIN).of(resourceType2).in(thing, anotherThing).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MIN).of(resourceType2).withTx(tx).in(anotherThing).execute();
            assertTrue(result.isEmpty());

            result = Graql.compute(MAX).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MAX).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MAX).of(resourceType2).in(thing, anotherThing).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MAX).of(resourceType2).withTx(tx).in(anotherThing).execute();
            assertTrue(result.isEmpty());
        }

        // connect entity and resources
        addResourceRelations();

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            result = tx.graql().compute(MIN).of(resourceType1).in(Collections.emptySet()).execute();
            assertEquals(1.2, result.get(0).number().doubleValue(), delta);
            result = Graql.compute(MIN).in(thing).of(resourceType2).withTx(tx).execute();
            assertEquals(-1, result.get(0).number().intValue());
            result = tx.graql().compute(MIN).in(thing).of(resourceType2, resourceType5).execute();
            assertEquals(-7, result.get(0).number().intValue());
            result = tx.graql().compute(MIN).in(thing, thing, thing).of(resourceType2, resourceType5).execute();
            assertEquals(-7, result.get(0).number().intValue());
            result = tx.graql().compute(MIN).in(anotherThing).of(resourceType2).execute();
            assertEquals(0, result.get(0).number().intValue());

            result = Graql.compute(MAX).withTx(tx).of(resourceType1).execute();
            assertEquals(1.8, result.get(0).number().doubleValue(), delta);
            result = tx.graql().compute(MAX).of(resourceType1, resourceType6).execute();
            assertEquals(7.5, result.get(0).number().doubleValue(), delta);
            result = tx.graql().compute(MAX).of(resourceType1, resourceType6).execute();
            assertEquals(7.5, result.get(0).number().doubleValue(), delta);
            result = tx.graql().compute(MAX).in(anotherThing).of(resourceType2).execute();
            assertEquals(0, result.get(0).number().intValue());
        }
    }

    @Test
    public void testSum() {
        List<Value> result;

        // resource-type has no instance
        addSchemaAndEntities();

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            result = Graql.compute(SUM).of(resourceType1).in(Collections.emptyList()).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(SUM).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(SUM).withTx(tx).of(resourceType1).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(SUM).withTx(tx).of(resourceType1).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(SUM).of(resourceType2).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(SUM).of(resourceType2, resourceType5).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(SUM).of(resourceType2).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(SUM).withTx(tx).of(resourceType2).execute();
            assertTrue(result.isEmpty());
        }

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            result = Graql.compute(SUM).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(SUM).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(SUM).of(resourceType2).in(thing, anotherThing).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(SUM).of(resourceType2).withTx(tx).in(anotherThing).execute();
            assertTrue(result.isEmpty());
        }

        // connect entity and resources
        addResourceRelations();

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            result = Graql.compute(SUM).of(resourceType1).withTx(tx).execute();
            assertEquals(4.5, result.get(0).number().doubleValue(), delta);
            result = Graql.compute(SUM).of(resourceType2).in(thing).withTx(tx).execute();
            assertEquals(3, result.get(0).number().intValue());
            result = tx.graql().compute(SUM).of(resourceType1, resourceType6).execute();
            assertEquals(27.0, result.get(0).number().doubleValue(), delta);
            result = tx.graql().compute(SUM).of(resourceType2, resourceType5).in(thing, anotherThing).execute();
            assertEquals(-18, result.get(0).number().intValue());
            result = tx.graql().compute(SUM).of(resourceType2, resourceType5).in(thing).execute();
            assertEquals(-11, result.get(0).number().intValue());
        }
    }

    @Test
    public void testMean() {
        List<Value> result;

        // resource-type has no instance
        addSchemaAndEntities();
        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            result = Graql.compute(MEAN).of(resourceType1).in(Collections.emptyList()).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MEAN).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MEAN).withTx(tx).of(resourceType1).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MEAN).withTx(tx).of(resourceType1).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MEAN).of(resourceType2).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MEAN).of(resourceType2, resourceType5).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MEAN).of(resourceType2).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MEAN).withTx(tx).of(resourceType2).execute();
            assertTrue(result.isEmpty());
        }

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            result = Graql.compute(MEAN).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MEAN).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MEAN).of(resourceType2).in(thing, anotherThing).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MEAN).of(resourceType2).withTx(tx).in(anotherThing).execute();
            assertTrue(result.isEmpty());
        }

        // connect entity and resources
        addResourceRelations();

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            result = Graql.compute(MEAN).withTx(tx).of(resourceType1).execute();
            assertEquals(1.5, result.get(0).number().doubleValue(), delta);
            result = Graql.compute(MEAN).of(resourceType2).withTx(tx).execute();
            assertEquals(1D, result.get(0).number().doubleValue(), delta);
            result = tx.graql().compute(MEAN).of(resourceType1, resourceType6).execute();
            assertEquals(4.5, result.get(0).number().doubleValue(), delta);
            result = tx.graql().compute(MEAN).in(thing, anotherThing).of(resourceType2, resourceType5).execute();
            assertEquals(-3D, result.get(0).number().doubleValue(), delta);
            result = tx.graql().compute(MEAN).in(thing).of(resourceType1, resourceType6).execute();
            assertEquals(3.9, result.get(0).number().doubleValue(), delta);
        }
    }

    @Test
    public void testStd() {
        List<Value> result;

        // resource-type has no instance
        addSchemaAndEntities();

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            result = Graql.compute(STD).of(resourceType1).in(Collections.emptyList()).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(STD).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(STD).withTx(tx).of(resourceType1).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(STD).withTx(tx).of(resourceType1).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(STD).of(resourceType2).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(STD).of(resourceType2, resourceType5).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(STD).of(resourceType2).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(STD).withTx(tx).of(resourceType2).execute();
            assertTrue(result.isEmpty());
        }

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            result = Graql.compute(STD).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(STD).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(STD).of(resourceType2).in(thing, anotherThing).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(STD).of(resourceType2).withTx(tx).in(anotherThing).execute();
            assertTrue(result.isEmpty());
        }

        // connect entity and resources
        addResourceRelations();

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            result = Graql.compute(STD).of(resourceType1).withTx(tx).execute();
            assertEquals(Math.sqrt(0.18 / 3), result.get(0).number().doubleValue(), delta);
            result = Graql.compute(STD).of(resourceType2).withTx(tx).in(anotherThing).execute();
            assertEquals(Math.sqrt(0D), result.get(0).number().doubleValue(), delta);
            result = tx.graql().compute(STD).of(resourceType1, resourceType6).execute();
            assertEquals(Math.sqrt(54.18 / 6), result.get(0).number().doubleValue(), delta);
            result = tx.graql().compute(STD).of(resourceType2, resourceType5).in(thing, anotherThing).execute();
            assertEquals(Math.sqrt(110.0 / 6), result.get(0).number().doubleValue(), delta);
            result = tx.graql().compute(STD).of(resourceType2).in(thing).execute();
            assertEquals(2.5, result.get(0).number().doubleValue(), delta);
        }

        List<Long> list = new ArrayList<>();
        long workerNumber = 3L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }

        List<Number> numberList = list.parallelStream().map(i -> {
            try (GraknTx tx = session.transaction(GraknTxType.READ)) {
                return tx.graql().compute(STD).of(resourceType2).in(thing).execute().get(0).number();
            }
        }).collect(Collectors.toList());
        numberList.forEach(value -> assertEquals(2.5D, value.doubleValue(), delta));
    }

    @Test
    public void testMedian() {
        List<Value> result;

        // resource-type has no instance
        addSchemaAndEntities();

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            result = Graql.compute(MEDIAN).of(resourceType1).in(Collections.emptyList()).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MEDIAN).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MEDIAN).withTx(tx).of(resourceType1).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MEDIAN).withTx(tx).of(resourceType1).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MEDIAN).of(resourceType2).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MEDIAN).of(resourceType2, resourceType5).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MEDIAN).of(resourceType2).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MEDIAN).withTx(tx).of(resourceType2).execute();
            assertTrue(result.isEmpty());
        }

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            result = Graql.compute(MEDIAN).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MEDIAN).of(resourceType1).withTx(tx).execute();
            assertTrue(result.isEmpty());
            result = tx.graql().compute(MEDIAN).of(resourceType2).in(thing, anotherThing).execute();
            assertTrue(result.isEmpty());
            result = Graql.compute(MEDIAN).of(resourceType2).withTx(tx).in(anotherThing).execute();
            assertTrue(result.isEmpty());
        }

        // connect entity and resources
        addResourceRelations();

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            result = tx.graql().compute(MEDIAN).of(resourceType1).execute();
            assertEquals(1.5D, result.get(0).number().doubleValue(), delta);
            result = Graql.compute(MEDIAN).withTx(tx).of(resourceType6).execute();
            assertEquals(7.5D, result.get(0).number().doubleValue(), delta);
            result = tx.graql().compute(MEDIAN).of(resourceType1, resourceType6).execute();
            assertEquals(1.8D, result.get(0).number().doubleValue(), delta);
            result = Graql.compute(MEDIAN).withTx(tx).of(resourceType2).execute();
            assertEquals(0L, result.get(0).number().longValue());
            result = Graql.compute(MEDIAN).withTx(tx).in(thing).of(resourceType5).execute();
            assertEquals(-7L, result.get(0).number().longValue());
            result = tx.graql().compute(MEDIAN).in(thing, anotherThing).of(resourceType2, resourceType5).execute();
            assertEquals(-7L, result.get(0).number().longValue());
            result = Graql.compute(MEDIAN).withTx(tx).in(thing).of(resourceType2).execute();
            assertNotEquals(0L, result.get(0).number().longValue());
        }

        List<Long> list = new ArrayList<>();
        long workerNumber = 3L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }

        List<Number> numberList = list.parallelStream().map(i -> {
            try (GraknTx tx = session.transaction(GraknTxType.READ)) {
                return tx.graql().compute(MEDIAN).of(resourceType1).execute().get(0).number();
            }
        }).collect(Collectors.toList());
        numberList.forEach(value -> assertEquals(1.5D, value.doubleValue(), delta));
    }

    @Test
    public void testHasResourceVerticesAndEdges() {
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {

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
            RelationshipType relationType = tx.putRelationshipType(Schema.ImplicitType.HAS.getLabel(Label.of("power")))
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

        Value result;

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            // No need to test all statistics as most of them share the same vertex program

            result = tx.graql().compute(MIN).of("power").execute().get(0);
            assertEquals(1L, result.number().longValue());

            result = tx.graql().compute(MAX).of("power").execute().get(0);
            assertEquals(3L, result.number().longValue());

            result = tx.graql().compute(SUM).of("power").execute().get(0);
            assertEquals(8L, result.number().longValue());

            result = tx.graql().compute(MEDIAN).of("power").execute().get(0);
            assertEquals(2L, result.number().longValue());
        }
    }

    private void addSchemaAndEntities() throws InvalidKBException {
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
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
            RelationshipType related = tx.putRelationshipType("related").relates(relation1).relates(relation2);

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
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
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
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
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

            RelationshipType relationshipType1 = tx.getSchemaConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType1)));
            relationshipType1.create()
                    .assign(resourceOwner1, entity1)
                    .assign(resourceValue1, tx.<Double>getAttributeType(resourceType1).create(1.2));
            relationshipType1.create()
                    .assign(resourceOwner1, entity1)
                    .assign(resourceValue1, tx.<Double>getAttributeType(resourceType1).create(1.5));
            relationshipType1.create()
                    .assign(resourceOwner1, entity3)
                    .assign(resourceValue1, tx.<Double>getAttributeType(resourceType1).create(1.8));

            RelationshipType relationshipType2 = tx.getSchemaConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType2)));
            relationshipType2.create()
                    .assign(resourceOwner2, entity1)
                    .assign(resourceValue2, tx.<Long>getAttributeType(resourceType2).create(4L));
            relationshipType2.create()
                    .assign(resourceOwner2, entity1)
                    .assign(resourceValue2, tx.<Long>getAttributeType(resourceType2).create(-1L));
            relationshipType2.create()
                    .assign(resourceOwner2, entity4)
                    .assign(resourceValue2, tx.<Long>getAttributeType(resourceType2).create(0L));

            tx.<Long>getAttributeType(resourceType3).create(100L);

            RelationshipType relationshipType5 = tx.getSchemaConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType5)));
            relationshipType5.create()
                    .assign(resourceOwner5, entity1)
                    .assign(resourceValue5, tx.<Long>getAttributeType(resourceType5).create(-7L));
            relationshipType5.create()
                    .assign(resourceOwner5, entity2)
                    .assign(resourceValue5, tx.<Long>getAttributeType(resourceType5).create(-7L));
            relationshipType5.create()
                    .assign(resourceOwner5, entity4)
                    .assign(resourceValue5, tx.<Long>getAttributeType(resourceType5).create(-7L));

            RelationshipType relationshipType6 = tx.getSchemaConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType6)));
            relationshipType6.create()
                    .assign(resourceOwner6, entity1)
                    .assign(resourceValue6, tx.<Double>getAttributeType(resourceType6).create(7.5));
            relationshipType6.create()
                    .assign(resourceOwner6, entity2)
                    .assign(resourceValue6, tx.<Double>getAttributeType(resourceType6).create(7.5));
            relationshipType6.create()
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