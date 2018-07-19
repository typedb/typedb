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
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Graql;
import ai.grakn.test.rule.SessionContext;
import ai.grakn.util.GraknTestUtil;
import ai.grakn.util.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.util.GraqlSyntax.Compute.Method.COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

public class CountTest {

    public GraknSession session;

    @ClassRule
    public final static SessionContext sessionContext = SessionContext.create();

    @Before
    public void setUp() {
        session = sessionContext.newSession();
    }

    @Test
    public void testCountAfterCommit() throws Exception {
        String nameThing = "thingy";
        String nameAnotherThing = "another";

        // assert the graph is empty
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            ComputeQuery count = Graql.compute(COUNT);
            Assert.assertEquals(0L, count.withTx(graph).execute().asNumber().get().longValue());
            Assert.assertEquals(0L, graph.graql().compute(COUNT).includeAttributes(true).execute().asNumber().get().longValue());
        }

        // add 2 instances
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType thingy = graph.putEntityType(nameThing);
            thingy.create();
            thingy.create();
            graph.commit();
        }

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            Assert.assertEquals(2L,
                    Graql.compute(COUNT).withTx(graph).in(nameThing).execute().asNumber().get().longValue());
        }

        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType anotherThing = graph.putEntityType(nameAnotherThing);
            anotherThing.create();
            graph.commit();
        }

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            // assert computer returns the correct count of instances
            Assert.assertEquals(2L,
                    Graql.compute(COUNT).withTx(graph).in(nameThing).includeAttributes(true).execute().asNumber().get().longValue());
            Assert.assertEquals(3L, graph.graql().compute(COUNT).execute().asNumber().get().longValue());
        }
    }

    @Test
    public void testConcurrentCount() throws Exception {
        assumeFalse(GraknTestUtil.usingTinker());

        String nameThing = "thingy";
        String nameAnotherThing = "another";

        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType thingy = graph.putEntityType(nameThing);
            thingy.create();
            thingy.create();
            EntityType anotherThing = graph.putEntityType(nameAnotherThing);
            anotherThing.create();
            graph.commit();
        }

        List<Long> list = new ArrayList<>(4);
        long workerNumber = 6L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }

        // running 4 jobs at the same time
        // collecting the result in the end so engine won't stop before the test finishes
        Set<Long> result;
        result = list.parallelStream()
                .map(i -> executeCount(session))
                .collect(Collectors.toSet());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(3L, result.iterator().next().longValue());

        result = list.parallelStream()
                .map(i -> executeCount(session))
                .collect(Collectors.toSet());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(3L, result.iterator().next().longValue());
    }

    @Test
    public void testHasResourceEdges() {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType person = graph.putEntityType("person");
            AttributeType<String> name = graph.putAttributeType("name", AttributeType.DataType.STRING);
            person.has(name);
            Entity aPerson = person.create();
            aPerson.has(name.create("jason"));
            graph.commit();
        }

        Optional<Number> count;
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            count = graph.graql().compute(COUNT).execute().asNumber();
            assertEquals(1L, count.get());

            count = graph.graql().compute(COUNT).includeAttributes(true).execute().asNumber();
            assertEquals(3L, count.get());

            count = graph.graql().compute(COUNT).in("name").execute().asNumber();
            assertEquals(1L, count.get());

            count = graph.graql().compute(COUNT).in("@has-name").execute().asNumber();
            assertEquals(1L, count.get());

            count = graph.graql().compute(COUNT).in("@has-name", "thing").execute().asNumber();
            assertEquals(3L, count.get());

            count = graph.graql().compute(COUNT).in("@has-name", "name").execute().asNumber();
            assertEquals(2L, count.get());

            count = graph.graql().compute(COUNT).in("relationship").execute().asNumber();
            assertEquals(0L, count.get());

            count = graph.graql().compute(COUNT).in("relationship").includeAttributes(true).execute().asNumber();
            assertEquals(1L, count.get());
        }

        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {

            // manually construct the relation type and instance
            EntityType person = graph.getEntityType("person");
            Entity aPerson = person.create();
            AttributeType<String> name = graph.putAttributeType("name", AttributeType.DataType.STRING);
            Attribute jason = name.create("jason");

            Role resourceOwner = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of("name")));
            person.plays(resourceOwner);
            Role resourceValue = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of("name")));
            name.plays(resourceValue);

            RelationshipType relationshipType =
                    graph.putRelationshipType(Schema.ImplicitType.HAS.getLabel(Label.of("name")))
                            .relates(resourceOwner).relates(resourceValue);
            relationshipType.create()
                    .assign(resourceOwner, aPerson)
                    .assign(resourceValue, jason);
            graph.commit();
        }

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            count = graph.graql().compute(COUNT).execute().asNumber();
            assertEquals(2L, count.get());

            count = graph.graql().compute(COUNT).includeAttributes(true).execute().asNumber();
            assertEquals(5L, count.get());

            count = graph.graql().compute(COUNT).in("name").execute().asNumber();
            assertEquals(1L, count.get());

            count = graph.graql().compute(COUNT).includeAttributes(true).in("name").execute().asNumber();
            assertEquals(1L, count.get());

            count = graph.graql().compute(COUNT).in("@has-name").execute().asNumber();
            assertEquals(2L, count.get());

            count = graph.graql().compute(COUNT).in("@has-name", "thing").execute().asNumber();
            assertEquals(5L, count.get());

            count = graph.graql().compute(COUNT).in("@has-name", "name").execute().asNumber();
            assertEquals(3L, count.get());

            count = graph.graql().compute(COUNT).in("relationship").execute().asNumber();
            assertEquals(0L, count.get());

            count = graph.graql().compute(COUNT).in("relationship").includeAttributes(true).execute().asNumber();
            assertEquals(2L, count.get());
        }
    }

    @Test
    public void testHasResourceVerticesAndEdges() {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {

            // manually construct the relation type and instance
            EntityType person = graph.putEntityType("person");
            Entity aPerson = person.create();
            AttributeType<String> name = graph.putAttributeType("name", AttributeType.DataType.STRING);
            Attribute jason = name.create("jason");

            person.has(name);

            Role resourceOwner = graph.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of("name")));
            person.plays(resourceOwner);
            Role resourceValue = graph.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of("name")));
            name.plays(resourceValue);

            RelationshipType relationshipType =
                    graph.putRelationshipType(Schema.ImplicitType.HAS.getLabel(Label.of("name")))
                            .relates(resourceOwner).relates(resourceValue);
            // here relationship type is still implicit
            relationshipType.create()
                    .assign(resourceOwner, aPerson)
                    .assign(resourceValue, jason);

            graph.commit();
        }

        Optional<Number> count;
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            count = graph.graql().compute(COUNT).execute().asNumber();
            assertEquals(1L, count.get());

            count = graph.graql().compute(COUNT).includeAttributes(true).execute().asNumber();
            assertEquals(3L, count.get());

            count = graph.graql().compute(COUNT).in("name").execute().asNumber();
            assertEquals(1L, count.get());

            count = graph.graql().compute(COUNT).in("@has-name").execute().asNumber();
            assertEquals(1L, count.get());

            count = graph.graql().compute(COUNT).in("@has-name", "name").execute().asNumber();
            assertEquals(2L, count.get());

            count = graph.graql().compute(COUNT).in("relationship").execute().asNumber();
            assertEquals(0L, count.get());

            count = graph.graql().compute(COUNT).in("relationship").includeAttributes(true).execute().asNumber();
            assertEquals(1L, count.get());
        }

        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType person = graph.getEntityType("person");
            AttributeType<String> name = graph.putAttributeType("name", AttributeType.DataType.STRING);
            Entity aPerson = person.create();
            aPerson.has(name.attribute("jason"));
            graph.commit();
        }

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            count = graph.graql().compute(COUNT).includeAttributes(true).execute().asNumber();
            assertEquals(5L, count.get());

            count = graph.graql().compute(COUNT).in("name").execute().asNumber();
            assertEquals(1L, count.get());

            count = graph.graql().compute(COUNT).in("@has-name").execute().asNumber();
            assertEquals(2L, count.get());

            count = graph.graql().compute(COUNT).in("@has-name", "name").execute().asNumber();
            assertEquals(3L, count.get());

            count = graph.graql().compute(COUNT).in("relationship").execute().asNumber();
            assertEquals(0L, count.get());

            count = graph.graql().compute(COUNT).in("relationship").includeAttributes(true).execute().asNumber();
            assertEquals(2L, count.get());
        }
    }

    private Long executeCount(GraknSession factory) {
        try (GraknTx graph = factory.transaction(GraknTxType.READ)) {
            return graph.graql().compute(COUNT).execute().asNumber().get().longValue();
        }
    }
}