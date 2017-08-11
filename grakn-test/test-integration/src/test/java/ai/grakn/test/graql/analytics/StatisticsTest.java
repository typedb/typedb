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
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.graql.Graql;
import ai.grakn.test.EngineContext;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

// TODO We can extend AbstractGraphTest instead when we remove persisting in analytics
public class StatisticsTest {

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

    @ClassRule
    public static final EngineContext context = EngineContext.startInMemoryServer();

    private GraknSession factory;

    @Before
    public void setUp() {
        factory = context.factoryWithNewKeyspace();
    }

    @Test
    public void testStatisticsExceptions() throws Exception {
        addOntologyAndEntities();
        addResourceRelations();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            //TODO: add more detailed error messages
            // resources-type is not set
            assertGraqlQueryExceptionThrown(graph.graql().compute().max().in(thing)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().min().in(thing)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().mean().in(thing)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().sum().in(thing)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().std().in(thing)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().median().in(thing)::execute);

            // if it's not a resource-type
            assertGraqlQueryExceptionThrown(graph.graql().compute().max().of(thing)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().min().of(thing)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().mean().of(thing)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().sum().of(thing)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().std().of(thing)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().median().of(thing)::execute);

            // resource-type has no instance
            assertFalse(graph.graql().compute().max().of(resourceType7).execute().isPresent());
            assertFalse(graph.graql().compute().min().of(resourceType7).execute().isPresent());
            assertFalse(graph.graql().compute().sum().of(resourceType7).execute().isPresent());
            assertFalse(graph.graql().compute().std().of(resourceType7).execute().isPresent());
            assertFalse(graph.graql().compute().median().of(resourceType7).execute().isPresent());
            assertFalse(graph.graql().compute().mean().of(resourceType7).execute().isPresent());

            // resources are not connected to any entities
            assertFalse(graph.graql().compute().max().of(resourceType3).execute().isPresent());
            assertFalse(graph.graql().compute().min().of(resourceType3).execute().isPresent());
            assertFalse(graph.graql().compute().sum().of(resourceType3).execute().isPresent());
            assertFalse(graph.graql().compute().std().of(resourceType3).execute().isPresent());
            assertFalse(graph.graql().compute().median().of(resourceType3).execute().isPresent());
            assertFalse(graph.graql().compute().mean().of(resourceType3).execute().isPresent());

            // resource-type has incorrect data type
            assertGraqlQueryExceptionThrown(graph.graql().compute().max().of(resourceType4)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().min().of(resourceType4)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().mean().of(resourceType4)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().sum().of(resourceType4)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().std().of(resourceType4)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().median().of(resourceType4)::execute);

            // resource-types have different data types
            Set<Label> resourceTypes = Sets.newHashSet(Label.of(resourceType1), Label.of(resourceType2));
            assertGraqlQueryExceptionThrown(graph.graql().compute().max().of(resourceTypes)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().min().of(resourceTypes)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().mean().of(resourceTypes)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().sum().of(resourceTypes)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().std().of(resourceTypes)::execute);
            assertGraqlQueryExceptionThrown(graph.graql().compute().median().of(resourceTypes)::execute);
        }
    }

    private void assertGraqlQueryExceptionThrown(Supplier<Optional> method) {
        boolean exceptionThrown = false;
        try {
            method.get();
        } catch (GraqlQueryException e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    @Test
    public void testMinAndMax() throws Exception {
        Optional<Number> result;

        // resource-type has no instance
        addOntologyAndEntities();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            result = Graql.compute().min().of(resourceType1).in(Collections.emptyList()).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().min().of(resourceType1).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().withGraph(graph).min().of(resourceType1).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().min().withGraph(graph).of(resourceType1).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().min().of(resourceType2).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().min().of(resourceType2, resourceType5).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().min().of(resourceType2).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().withGraph(graph).min().of(resourceType2).execute();
            assertFalse(result.isPresent());

            result = Graql.compute().max().of(resourceType1).in(Collections.emptyList()).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().max().of(resourceType1).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().withGraph(graph).max().of(resourceType1).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().max().withGraph(graph).of(resourceType1).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().max().of(resourceType2).in(Collections.emptyList()).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().max().of(resourceType2, resourceType5).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().max().of(resourceType2).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().withGraph(graph).max().of(resourceType2).execute();
            assertFalse(result.isPresent());
        }

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            result = Graql.compute().min().of(resourceType1).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().min().of(resourceType1).in().withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().min().of(resourceType2).in(thing, anotherThing).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().min().of(resourceType2).withGraph(graph).in(anotherThing).execute();
            assertFalse(result.isPresent());

            result = Graql.compute().max().of(resourceType1).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().max().of(resourceType1).in().withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().max().of(resourceType2).in(thing, anotherThing).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().max().of(resourceType2).withGraph(graph).in(anotherThing).execute();
            assertFalse(result.isPresent());
        }

        // connect entity and resources
        addResourceRelations();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            result = graph.graql().compute().min().of(resourceType1).in(Collections.emptySet()).execute();
            assertEquals(1.2, result.get().doubleValue(), delta);
            result = Graql.compute().min().in(thing).of(resourceType2).withGraph(graph).execute();
            assertEquals(-1L, result.get());
            result = graph.graql().compute().min().in(thing).of(resourceType2, resourceType5).execute();
            assertEquals(-7L, result.get());
            result = graph.graql().compute().min().in(thing, thing, thing).of(resourceType2, resourceType5).execute();
            assertEquals(-7L, result.get());
            result = graph.graql().compute().min().in(anotherThing).of(resourceType2).execute();
            assertEquals(0L, result.get());

            result = Graql.compute().max().in().withGraph(graph).of(resourceType1).execute();
            assertEquals(1.8, result.get().doubleValue(), delta);
            result = graph.graql().compute().max().of(resourceType1, resourceType6).execute();
            assertEquals(7.5, result.get().doubleValue(), delta);
            result = graph.graql().compute().max().of(resourceType1, resourceType6).execute();
            assertEquals(7.5, result.get().doubleValue(), delta);
            result = graph.graql().compute().max().in(anotherThing).of(resourceType2).execute();
            assertEquals(0L, result.get());
        }
    }

    @Test
    public void testSum() throws Exception {
        Optional<Number> result;

        // resource-type has no instance
        addOntologyAndEntities();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            result = Graql.compute().sum().of(resourceType1).in(Collections.emptyList()).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().sum().of(resourceType1).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().withGraph(graph).sum().of(resourceType1).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().sum().withGraph(graph).of(resourceType1).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().sum().of(resourceType2).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().sum().of(resourceType2, resourceType5).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().sum().of(resourceType2).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().withGraph(graph).sum().of(resourceType2).execute();
            assertFalse(result.isPresent());
        }

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            result = Graql.compute().sum().of(resourceType1).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().sum().of(resourceType1).in().withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().sum().of(resourceType2).in(thing, anotherThing).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().sum().of(resourceType2).withGraph(graph).in(anotherThing).execute();
            assertFalse(result.isPresent());
        }

        // connect entity and resources
        addResourceRelations();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            result = Graql.compute().sum().of(resourceType1).withGraph(graph).execute();
            assertEquals(4.5, result.get().doubleValue(), delta);
            result = Graql.compute().sum().of(resourceType2).in(thing).withGraph(graph).execute();
            assertEquals(3L, result.get());
            result = graph.graql().compute().sum().of(resourceType1, resourceType6).execute();
            assertEquals(27.0, result.get().doubleValue(), delta);
            result = graph.graql().compute().sum().of(resourceType2, resourceType5).in(thing, anotherThing).execute();
            assertEquals(-18L, result.get());
            result = graph.graql().compute().sum().of(resourceType2, resourceType5).in(thing).execute();
            assertEquals(-11L, result.get());
        }
    }

    @Test
    public void testMean() throws Exception {
        Optional<Double> result;

        // resource-type has no instance
        addOntologyAndEntities();
        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            result = Graql.compute().mean().of(resourceType1).in(Collections.emptyList()).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().mean().of(resourceType1).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().withGraph(graph).mean().of(resourceType1).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().mean().withGraph(graph).of(resourceType1).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().mean().of(resourceType2).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().mean().of(resourceType2, resourceType5).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().mean().of(resourceType2).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().withGraph(graph).mean().of(resourceType2).execute();
            assertFalse(result.isPresent());
        }

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            result = Graql.compute().mean().of(resourceType1).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().mean().of(resourceType1).in().withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().mean().of(resourceType2).in(thing, anotherThing).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().mean().of(resourceType2).withGraph(graph).in(anotherThing).execute();
            assertFalse(result.isPresent());
        }

        // connect entity and resources
        addResourceRelations();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            result = Graql.compute().withGraph(graph).mean().of(resourceType1).execute();
            assertEquals(1.5, result.get(), delta);
            result = Graql.compute().mean().of(resourceType2).withGraph(graph).execute();
            assertEquals(1D, result.get(), delta);
            result = graph.graql().compute().mean().of(resourceType1, resourceType6).execute();
            assertEquals(4.5, result.get(), delta);
            result = graph.graql().compute().mean().in(thing, anotherThing).of(resourceType2, resourceType5).execute();
            assertEquals(-3D, result.get(), delta);
            result = graph.graql().compute().mean().in(thing).of(resourceType1, resourceType6).execute();
            assertEquals(3.9, result.get(), delta);
        }
    }

    @Test
    public void testStd() throws Exception {
        Optional<Double> result;

        // resource-type has no instance
        addOntologyAndEntities();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            result = Graql.compute().std().of(resourceType1).in(Collections.emptyList()).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().std().of(resourceType1).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().withGraph(graph).std().of(resourceType1).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().std().withGraph(graph).of(resourceType1).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().std().of(resourceType2).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().std().of(resourceType2, resourceType5).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().std().of(resourceType2).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().withGraph(graph).std().of(resourceType2).execute();
            assertFalse(result.isPresent());
        }

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            result = Graql.compute().std().of(resourceType1).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().std().of(resourceType1).in().withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().std().of(resourceType2).in(thing, anotherThing).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().std().of(resourceType2).withGraph(graph).in(anotherThing).execute();
            assertFalse(result.isPresent());
        }

        // connect entity and resources
        addResourceRelations();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            result = Graql.compute().std().of(resourceType1).withGraph(graph).execute();
            assertEquals(Math.sqrt(0.18 / 3), result.get(), delta);
            result = Graql.compute().std().of(resourceType2).withGraph(graph).in(anotherThing).execute();
            assertEquals(Math.sqrt(0D), result.get(), delta);
            result = graph.graql().compute().std().of(resourceType1, resourceType6).execute();
            assertEquals(Math.sqrt(54.18 / 6), result.get(), delta);
            result = graph.graql().compute().std().of(resourceType2, resourceType5).in(thing, anotherThing).execute();
            assertEquals(Math.sqrt(110.0 / 6), result.get(), delta);
            result = graph.graql().compute().std().of(resourceType2).in(thing).execute();
            assertEquals(2.5, result.get(), delta);
        }

        List<Long> list = new ArrayList<>();
        long workerNumber = 3L;
        if (GraknTestSetup.usingTinker()) workerNumber = 1;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }

        List<Double> numberList = list.parallelStream().map(i -> {
            try (GraknGraph graph = factory.open(GraknTxType.READ)) {
                return graph.graql().compute().std().of(resourceType2).in(thing).execute().get();
            }
        }).collect(Collectors.toList());
        numberList.forEach(value -> assertEquals(2.5D, value.doubleValue(), delta));
    }

    @Test
    public void testMedian() throws Exception {
        Optional<Number> result;

        // resource-type has no instance
        addOntologyAndEntities();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            result = Graql.compute().median().of(resourceType1).in(Collections.emptyList()).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().median().of(resourceType1).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().withGraph(graph).median().of(resourceType1).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().median().withGraph(graph).of(resourceType1).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().median().of(resourceType2).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().median().of(resourceType2, resourceType5).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().median().of(resourceType2).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().withGraph(graph).median().of(resourceType2).execute();
            assertFalse(result.isPresent());
        }

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            result = Graql.compute().median().of(resourceType1).withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().median().of(resourceType1).in().withGraph(graph).execute();
            assertFalse(result.isPresent());
            result = graph.graql().compute().median().of(resourceType2).in(thing, anotherThing).execute();
            assertFalse(result.isPresent());
            result = Graql.compute().median().of(resourceType2).withGraph(graph).in(anotherThing).execute();
            assertFalse(result.isPresent());
        }

        // connect entity and resources
        addResourceRelations();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            result = graph.graql().compute().median().of(resourceType1).in().execute();
            assertEquals(1.5D, result.get().doubleValue(), delta);
            result = Graql.compute().withGraph(graph).median().of(resourceType6).execute();
            assertEquals(7.5D, result.get().doubleValue(), delta);
            result = graph.graql().compute().median().of(resourceType1, resourceType6).execute();
            assertEquals(1.8D, result.get().doubleValue(), delta);
            result = Graql.compute().withGraph(graph).median().of(resourceType2).execute();
            assertEquals(0L, result.get().longValue());
            result = Graql.compute().withGraph(graph).median().in(thing).of(resourceType5).execute();
            assertEquals(-7L, result.get().longValue());
            result = graph.graql().compute().median().in(thing, anotherThing).of(resourceType2, resourceType5).execute();
            assertEquals(-7L, result.get().longValue());
            result = Graql.compute().withGraph(graph).median().in(thing).of(resourceType2).execute();
            assertNotEquals(0L, result.get().longValue());
        }

        List<Long> list = new ArrayList<>();
        long workerNumber = 3L;
        if (GraknTestSetup.usingTinker()) workerNumber = 1;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }

        List<Number> numberList = list.parallelStream().map(i -> {
            try (GraknGraph graph = factory.open(GraknTxType.READ)) {
                return graph.graql().compute().median().of(resourceType1).execute().get();
            }
        }).collect(Collectors.toList());
        numberList.forEach(value -> assertEquals(1.5D, value.doubleValue(), delta));
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

            Role relation1 = graph.putRole("relation1");
            Role relation2 = graph.putRole("relation2");
            entityType1.plays(relation1).plays(relation2);
            entityType2.plays(relation1).plays(relation2);
            RelationType related = graph.putRelationType("related").relates(relation1).relates(relation2);

            related.addRelation()
                    .addRolePlayer(relation1, entity1)
                    .addRolePlayer(relation2, entity2);
            related.addRelation()
                    .addRolePlayer(relation1, entity2)
                    .addRolePlayer(relation2, entity3);
            related.addRelation()
                    .addRolePlayer(relation1, entity2)
                    .addRolePlayer(relation2, entity4);

            List<ResourceType> resourceTypeList = new ArrayList<>();
            resourceTypeList.add(graph.putResourceType(resourceType1, ResourceType.DataType.DOUBLE));
            resourceTypeList.add(graph.putResourceType(resourceType2, ResourceType.DataType.LONG));
            resourceTypeList.add(graph.putResourceType(resourceType3, ResourceType.DataType.LONG));
            resourceTypeList.add(graph.putResourceType(resourceType4, ResourceType.DataType.STRING));
            resourceTypeList.add(graph.putResourceType(resourceType5, ResourceType.DataType.LONG));
            resourceTypeList.add(graph.putResourceType(resourceType6, ResourceType.DataType.DOUBLE));
            resourceTypeList.add(graph.putResourceType(resourceType7, ResourceType.DataType.DOUBLE));

            Role resourceOwner1 = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType1)));
            Role resourceOwner2 = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType2)));
            Role resourceOwner3 = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType3)));
            Role resourceOwner4 = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType4)));
            Role resourceOwner5 = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType5)));
            Role resourceOwner6 = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType6)));
            Role resourceOwner7 = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of(resourceType7)));

            Role resourceValue1 = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType1)));
            Role resourceValue2 = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType2)));
            Role resourceValue3 = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType3)));
            Role resourceValue4 = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType4)));
            Role resourceValue5 = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType5)));
            Role resourceValue6 = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType6)));
            Role resourceValue7 = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of(resourceType7)));

            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType1)))
                    .relates(resourceOwner1).relates(resourceValue1);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType2)))
                    .relates(resourceOwner2).relates(resourceValue2);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType3)))
                    .relates(resourceOwner3).relates(resourceValue3);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType4)))
                    .relates(resourceOwner4).relates(resourceValue4);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType5)))
                    .relates(resourceOwner5).relates(resourceValue5);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType6)))
                    .relates(resourceOwner6).relates(resourceValue6);
            graph.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType7)))
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

    private void addResourcesInstances() throws InvalidGraphException {
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            graph.<Double>getResourceType(resourceType1).putResource(1.2);
            graph.<Double>getResourceType(resourceType1).putResource(1.5);
            graph.<Double>getResourceType(resourceType1).putResource(1.8);

            graph.<Long>getResourceType(resourceType2).putResource(4L);
            graph.<Long>getResourceType(resourceType2).putResource(-1L);
            graph.<Long>getResourceType(resourceType2).putResource(0L);

            graph.<Long>getResourceType(resourceType5).putResource(6L);
            graph.<Long>getResourceType(resourceType5).putResource(7L);
            graph.<Long>getResourceType(resourceType5).putResource(8L);

            graph.<Double>getResourceType(resourceType6).putResource(7.2);
            graph.<Double>getResourceType(resourceType6).putResource(7.5);
            graph.<Double>getResourceType(resourceType6).putResource(7.8);

            graph.<String>getResourceType(resourceType4).putResource("a");
            graph.<String>getResourceType(resourceType4).putResource("b");
            graph.<String>getResourceType(resourceType4).putResource("c");

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
                    .addRolePlayer(resourceValue1, graph.<Double>getResourceType(resourceType1).putResource(1.2));
            relationType1.addRelation()
                    .addRolePlayer(resourceOwner1, entity1)
                    .addRolePlayer(resourceValue1, graph.<Double>getResourceType(resourceType1).putResource(1.5));
            relationType1.addRelation()
                    .addRolePlayer(resourceOwner1, entity3)
                    .addRolePlayer(resourceValue1, graph.<Double>getResourceType(resourceType1).putResource(1.8));

            RelationType relationType2 = graph.getOntologyConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType2)));
            relationType2.addRelation()
                    .addRolePlayer(resourceOwner2, entity1)
                    .addRolePlayer(resourceValue2, graph.<Long>getResourceType(resourceType2).putResource(4L));
            relationType2.addRelation()
                    .addRolePlayer(resourceOwner2, entity1)
                    .addRolePlayer(resourceValue2, graph.<Long>getResourceType(resourceType2).putResource(-1L));
            relationType2.addRelation()
                    .addRolePlayer(resourceOwner2, entity4)
                    .addRolePlayer(resourceValue2, graph.<Long>getResourceType(resourceType2).putResource(0L));

            graph.<Long>getResourceType(resourceType3).putResource(100L);

            RelationType relationType5 = graph.getOntologyConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType5)));
            relationType5.addRelation()
                    .addRolePlayer(resourceOwner5, entity1)
                    .addRolePlayer(resourceValue5, graph.<Long>getResourceType(resourceType5).putResource(-7L));
            relationType5.addRelation()
                    .addRolePlayer(resourceOwner5, entity2)
                    .addRolePlayer(resourceValue5, graph.<Long>getResourceType(resourceType5).putResource(-7L));
            relationType5.addRelation()
                    .addRolePlayer(resourceOwner5, entity4)
                    .addRolePlayer(resourceValue5, graph.<Long>getResourceType(resourceType5).putResource(-7L));

            RelationType relationType6 = graph.getOntologyConcept(Schema.ImplicitType.HAS.getLabel(Label.of(resourceType6)));
            relationType6.addRelation()
                    .addRolePlayer(resourceOwner6, entity1)
                    .addRolePlayer(resourceValue6, graph.<Double>getResourceType(resourceType6).putResource(7.5));
            relationType6.addRelation()
                    .addRolePlayer(resourceOwner6, entity2)
                    .addRolePlayer(resourceValue6, graph.<Double>getResourceType(resourceType6).putResource(7.5));
            relationType6.addRelation()
                    .addRolePlayer(resourceOwner6, entity4)
                    .addRolePlayer(resourceValue6, graph.<Double>getResourceType(resourceType6).putResource(7.5));

            // some resources in, but not connect them to any instances
            graph.<Double>getResourceType(resourceType1).putResource(2.8);
            graph.<Long>getResourceType(resourceType2).putResource(-5L);
            graph.<Long>getResourceType(resourceType5).putResource(10L);
            graph.<Double>getResourceType(resourceType6).putResource(0.8);

            graph.commit();
        }
    }
}
