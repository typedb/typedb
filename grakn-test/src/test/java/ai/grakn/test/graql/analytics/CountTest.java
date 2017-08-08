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
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.graql.Graql;
import ai.grakn.test.EngineContext;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.util.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class CountTest {

    @ClassRule
    public static final EngineContext rule = EngineContext.startInMemoryServer();

    private GraknSession factory;

    @Before
    public void setUp() {
        factory = rule.factoryWithNewKeyspace();
    }

    @Test
    public void testCountAfterCommit() throws Exception {
        String nameThing = "thingy";
        String nameAnotherThing = "another";

        // assert the graph is empty
        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            Assert.assertEquals(0L, Graql.compute().count().withGraph(graph).execute().longValue());
            Assert.assertEquals(0L, graph.graql().compute().count().execute().longValue());
        }

        // add 2 instances
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            EntityType thingy = graph.putEntityType(nameThing);
            thingy.addEntity();
            thingy.addEntity();
            graph.commit();
        }

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            Assert.assertEquals(2L,
                    Graql.compute().withGraph(graph).count().in(nameThing).execute().longValue());
        }

        // create 1 more, rdd is refreshed
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            EntityType anotherThing = graph.putEntityType(nameAnotherThing);
            anotherThing.addEntity();
            graph.commit();
        }

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            // assert computer returns the correct count of instances
            Assert.assertEquals(2L,
                    Graql.compute().withGraph(graph).count().in(nameThing).execute().longValue());
            Assert.assertEquals(3L, graph.graql().compute().count().execute().longValue());
        }

        List<Long> list = new ArrayList<>(4);
        long workerNumber = 6L;
        if (GraknTestSetup.usingTinker()) workerNumber = 1L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }

        // running 4 jobs at the same time
        // collecting the result in the end so engine won't stop before the test finishes
        Set<Long> result;
        result = list.parallelStream()
                .map(i -> executeCount(factory))
                .collect(Collectors.toSet());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(3L, result.iterator().next().longValue());

        result = list.parallelStream()
                .map(i -> executeCount(factory))
                .collect(Collectors.toSet());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(3L, result.iterator().next().longValue());

    }

    @Test
    public void testDegreeWithHasResourceEdges() {
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            EntityType person = graph.putEntityType("person");
            ResourceType<String> name = graph.putResourceType("name", ResourceType.DataType.STRING);
            person.resource(name);
            Entity aPerson = person.addEntity();
            aPerson.resource(name.putResource("jason"));
            graph.commit();
        }

        long count;
        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            count = graph.graql().compute().count().execute();
            assertEquals(count, 3L);

            count = graph.graql().compute().count().in("name").execute();
            assertEquals(count, 1L);

            count = graph.graql().compute().count().in("has-name").execute();
            assertEquals(count, 1L);

            count = graph.graql().compute().count().in("has-name", "name").execute();
            assertEquals(count, 2L);
        }
    }

    @Test
    public void testDegreeWithHasResourceVertices() {
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {

            // manually construct the relation type and instance
            EntityType person = graph.putEntityType("person");
            Entity aPerson = person.addEntity();
            ResourceType<String> name = graph.putResourceType("name", ResourceType.DataType.STRING);
            Resource jason = name.putResource("jason");

            Role resourceOwner = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of("name")));
            person.plays(resourceOwner);
            Role resourceValue = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of("name")));
            name.plays(resourceValue);

            RelationType relationType = graph.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of("name")))
                    .relates(resourceOwner).relates(resourceValue);
            relationType.addRelation()
                    .addRolePlayer(resourceOwner, aPerson)
                    .addRolePlayer(resourceValue, jason);
            graph.commit();
        }

        long count;
        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            count = graph.graql().compute().count().execute();
            assertEquals(count, 3L);

            count = graph.graql().compute().count().in("name").execute();
            assertEquals(count, 1L);

            count = graph.graql().compute().count().in("has-name").execute();
            assertEquals(count, 1L);

            count = graph.graql().compute().count().in("has-name", "name").execute();
            assertEquals(count, 2L);
        }
    }

    private Long executeCount(GraknSession factory) {
        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            return graph.graql().compute().count().execute();
        }
    }
}