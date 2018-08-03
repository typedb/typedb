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
import ai.grakn.graql.answer.Value;
import ai.grakn.test.rule.SessionContext;
import ai.grakn.util.GraknTestUtil;
import ai.grakn.util.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
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
            Assert.assertEquals(0, graph.graql().compute(COUNT).execute().get(0).number().intValue());
            Assert.assertEquals(0, graph.graql().compute(COUNT).includeAttributes(true).execute().get(0).number().intValue());
        }

        // add 2 instances
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType thingy = graph.putEntityType(nameThing);
            thingy.create();
            thingy.create();
            graph.commit();
        }

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            Assert.assertEquals(2, graph.graql().compute(COUNT).in(nameThing).execute().get(0).number().intValue());
        }

        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType anotherThing = graph.putEntityType(nameAnotherThing);
            anotherThing.create();
            graph.commit();
        }

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            // assert computer returns the correct count of instances
            Assert.assertEquals(2, graph.graql().compute(COUNT).in(nameThing).includeAttributes(true).execute().get(0).number().intValue());
            Assert.assertEquals(3, graph.graql().compute(COUNT).execute().get(0).number().intValue());
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

        Value count;
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            count = graph.graql().compute(COUNT).execute().get(0);
            assertEquals(1, count.number().intValue());

            count = graph.graql().compute(COUNT).includeAttributes(true).execute().get(0);
            assertEquals(3L, count.number().intValue());

            count = graph.graql().compute(COUNT).in("name").execute().get(0);
            assertEquals(1, count.number().intValue());

            count = graph.graql().compute(COUNT).in("@has-name").execute().get(0);
            assertEquals(1, count.number().intValue());

            count = graph.graql().compute(COUNT).in("@has-name", "thing").execute().get(0);
            assertEquals(3, count.number().intValue());

            count = graph.graql().compute(COUNT).in("@has-name", "name").execute().get(0);
            assertEquals(2, count.number().intValue());

            count = graph.graql().compute(COUNT).in("relationship").execute().get(0);
            assertEquals(0, count.number().intValue());

            count = graph.graql().compute(COUNT).in("relationship").includeAttributes(true).execute().get(0);
            assertEquals(1, count.number().intValue());
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
            count = graph.graql().compute(COUNT).execute().get(0);
            assertEquals(2, count.number().intValue());

            count = graph.graql().compute(COUNT).includeAttributes(true).execute().get(0);
            assertEquals(5, count.number().intValue());

            count = graph.graql().compute(COUNT).in("name").execute().get(0);
            assertEquals(1, count.number().intValue());

            count = graph.graql().compute(COUNT).includeAttributes(true).in("name").execute().get(0);
            assertEquals(1, count.number().intValue());

            count = graph.graql().compute(COUNT).in("@has-name").execute().get(0);
            assertEquals(2, count.number().intValue());

            count = graph.graql().compute(COUNT).in("@has-name", "thing").execute().get(0);
            assertEquals(5, count.number().intValue());

            count = graph.graql().compute(COUNT).in("@has-name", "name").execute().get(0);
            assertEquals(3, count.number().intValue());

            count = graph.graql().compute(COUNT).in("relationship").execute().get(0);
            assertEquals(0, count.number().intValue());

            count = graph.graql().compute(COUNT).in("relationship").includeAttributes(true).execute().get(0);
            assertEquals(2, count.number().intValue());
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

        Value count;
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            count = graph.graql().compute(COUNT).execute().get(0);
            assertEquals(1, count.number().intValue());

            count = graph.graql().compute(COUNT).includeAttributes(true).execute().get(0);
            assertEquals(3, count.number().intValue());

            count = graph.graql().compute(COUNT).in("name").execute().get(0);
            assertEquals(1, count.number().intValue());

            count = graph.graql().compute(COUNT).in("@has-name").execute().get(0);
            assertEquals(1, count.number().intValue());

            count = graph.graql().compute(COUNT).in("@has-name", "name").execute().get(0);
            assertEquals(2, count.number().intValue());

            count = graph.graql().compute(COUNT).in("relationship").execute().get(0);
            assertEquals(0, count.number().intValue());

            count = graph.graql().compute(COUNT).in("relationship").includeAttributes(true).execute().get(0);
            assertEquals(1, count.number().intValue());
        }

        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType person = graph.getEntityType("person");
            AttributeType<String> name = graph.putAttributeType("name", AttributeType.DataType.STRING);
            Entity aPerson = person.create();
            aPerson.has(name.attribute("jason"));
            graph.commit();
        }

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            count = graph.graql().compute(COUNT).includeAttributes(true).execute().get(0);
            assertEquals(5, count.number().intValue());

            count = graph.graql().compute(COUNT).in("name").execute().get(0);
            assertEquals(1, count.number().intValue());

            count = graph.graql().compute(COUNT).in("@has-name").execute().get(0);
            assertEquals(2, count.number().intValue());

            count = graph.graql().compute(COUNT).in("@has-name", "name").execute().get(0);
            assertEquals(3, count.number().intValue());

            count = graph.graql().compute(COUNT).in("relationship").execute().get(0);
            assertEquals(0, count.number().intValue());

            count = graph.graql().compute(COUNT).in("relationship").includeAttributes(true).execute().get(0);
            assertEquals(2, count.number().intValue());
        }
    }

    private Long executeCount(GraknSession session) {
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            return graph.graql().compute(COUNT).execute().get(0).number().longValue();
        }
    }
}