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
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.graql.answer.Value;
import ai.grakn.test.rule.EmbeddedCassandraContext;
import ai.grakn.test.rule.ServerContext;
import ai.grakn.util.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.util.GraqlSyntax.Compute.Method.COUNT;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("CheckReturnValue")
public class CountIT {

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
    public void testCountAfterCommit() {
        String nameThing = "thingy";
        String nameAnotherThing = "another";

        // assert the tx is empty
        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            Assert.assertEquals(0, tx.graql().compute(COUNT).execute().get(0).number().intValue());
            Assert.assertEquals(0, tx.graql().compute(COUNT).includeAttributes(true).execute().get(0).number().intValue());
        }

        // add 2 instances
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            EntityType thingy = tx.putEntityType(nameThing);
            thingy.create();
            thingy.create();
            tx.commit();
        }

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            Assert.assertEquals(2, tx.graql().compute(COUNT).in(nameThing).execute().get(0).number().intValue());
        }

        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            EntityType anotherThing = tx.putEntityType(nameAnotherThing);
            anotherThing.create();
            tx.commit();
        }

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            // assert computer returns the correct count of instances
            Assert.assertEquals(2, tx.graql().compute(COUNT).in(nameThing).includeAttributes(true).execute().get(0).number().intValue());
            Assert.assertEquals(3, tx.graql().compute(COUNT).execute().get(0).number().intValue());
        }
    }

    @Test
    public void testConcurrentCount() {
        String nameThing = "thingy";
        String nameAnotherThing = "another";

        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            EntityType thingy = tx.putEntityType(nameThing);
            thingy.create();
            thingy.create();
            EntityType anotherThing = tx.putEntityType(nameAnotherThing);
            anotherThing.create();
            tx.commit();
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
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            EntityType person = tx.putEntityType("person");
            AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
            person.has(name);
            Entity aPerson = person.create();
            aPerson.has(name.create("jason"));
            tx.commit();
        }

        Value count;
        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            count = tx.graql().compute(COUNT).execute().get(0);
            assertEquals(1, count.number().intValue());

            count = tx.graql().compute(COUNT).includeAttributes(true).execute().get(0);
            assertEquals(3L, count.number().intValue());

            count = tx.graql().compute(COUNT).in("name").execute().get(0);
            assertEquals(1, count.number().intValue());

            count = tx.graql().compute(COUNT).in("@has-name").execute().get(0);
            assertEquals(1, count.number().intValue());

            count = tx.graql().compute(COUNT).in("@has-name", "thing").execute().get(0);
            assertEquals(3, count.number().intValue());

            count = tx.graql().compute(COUNT).in("@has-name", "name").execute().get(0);
            assertEquals(2, count.number().intValue());

            count = tx.graql().compute(COUNT).in("relationship").execute().get(0);
            assertEquals(0, count.number().intValue());

            count = tx.graql().compute(COUNT).in("relationship").includeAttributes(true).execute().get(0);
            assertEquals(1, count.number().intValue());
        }

        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {

            // manually construct the relation type and instance
            EntityType person = tx.getEntityType("person");
            Entity aPerson = person.create();
            AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
            Attribute jason = name.create("jason");

            Role resourceOwner = tx.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of("name")));
            person.plays(resourceOwner);
            Role resourceValue = tx.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of("name")));
            name.plays(resourceValue);

            RelationshipType relationshipType =
                    tx.putRelationshipType(Schema.ImplicitType.HAS.getLabel(Label.of("name")))
                            .relates(resourceOwner).relates(resourceValue);
            relationshipType.create()
                    .assign(resourceOwner, aPerson)
                    .assign(resourceValue, jason);
            tx.commit();
        }

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            count = tx.graql().compute(COUNT).execute().get(0);
            assertEquals(2, count.number().intValue());

            count = tx.graql().compute(COUNT).includeAttributes(true).execute().get(0);
            assertEquals(5, count.number().intValue());

            count = tx.graql().compute(COUNT).in("name").execute().get(0);
            assertEquals(1, count.number().intValue());

            count = tx.graql().compute(COUNT).includeAttributes(true).in("name").execute().get(0);
            assertEquals(1, count.number().intValue());

            count = tx.graql().compute(COUNT).in("@has-name").execute().get(0);
            assertEquals(2, count.number().intValue());

            count = tx.graql().compute(COUNT).in("@has-name", "thing").execute().get(0);
            assertEquals(5, count.number().intValue());

            count = tx.graql().compute(COUNT).in("@has-name", "name").execute().get(0);
            assertEquals(3, count.number().intValue());

            count = tx.graql().compute(COUNT).in("relationship").execute().get(0);
            assertEquals(0, count.number().intValue());

            count = tx.graql().compute(COUNT).in("relationship").includeAttributes(true).execute().get(0);
            assertEquals(2, count.number().intValue());
        }
    }

    @Test
    public void testHasResourceVerticesAndEdges() {
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {

            // manually construct the relation type and instance
            EntityType person = tx.putEntityType("person");
            Entity aPerson = person.create();
            AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
            Attribute jason = name.create("jason");

            person.has(name);

            Role resourceOwner = tx.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of("name")));
            person.plays(resourceOwner);
            Role resourceValue = tx.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of("name")));
            name.plays(resourceValue);

            RelationshipType relationshipType =
                    tx.putRelationshipType(Schema.ImplicitType.HAS.getLabel(Label.of("name")))
                            .relates(resourceOwner).relates(resourceValue);
            // here relationship type is still implicit
            relationshipType.create()
                    .assign(resourceOwner, aPerson)
                    .assign(resourceValue, jason);

            tx.commit();
        }

        Value count;
        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            count = tx.graql().compute(COUNT).execute().get(0);
            assertEquals(1, count.number().intValue());

            count = tx.graql().compute(COUNT).includeAttributes(true).execute().get(0);
            assertEquals(3, count.number().intValue());

            count = tx.graql().compute(COUNT).in("name").execute().get(0);
            assertEquals(1, count.number().intValue());

            count = tx.graql().compute(COUNT).in("@has-name").execute().get(0);
            assertEquals(1, count.number().intValue());

            count = tx.graql().compute(COUNT).in("@has-name", "name").execute().get(0);
            assertEquals(2, count.number().intValue());

            count = tx.graql().compute(COUNT).in("relationship").execute().get(0);
            assertEquals(0, count.number().intValue());

            count = tx.graql().compute(COUNT).in("relationship").includeAttributes(true).execute().get(0);
            assertEquals(1, count.number().intValue());
        }

        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            EntityType person = tx.getEntityType("person");
            AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
            Entity aPerson = person.create();
            aPerson.has(name.attribute("jason"));
            tx.commit();
        }

        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            count = tx.graql().compute(COUNT).includeAttributes(true).execute().get(0);
            assertEquals(5, count.number().intValue());

            count = tx.graql().compute(COUNT).in("name").execute().get(0);
            assertEquals(1, count.number().intValue());

            count = tx.graql().compute(COUNT).in("@has-name").execute().get(0);
            assertEquals(2, count.number().intValue());

            count = tx.graql().compute(COUNT).in("@has-name", "name").execute().get(0);
            assertEquals(3, count.number().intValue());

            count = tx.graql().compute(COUNT).in("relationship").execute().get(0);
            assertEquals(0, count.number().intValue());

            count = tx.graql().compute(COUNT).in("relationship").includeAttributes(true).execute().get(0);
            assertEquals(2, count.number().intValue());
        }
    }

    private Long executeCount(GraknSession session) {
        try (GraknTx tx = session.transaction(GraknTxType.READ)) {
            return tx.graql().compute(COUNT).execute().get(0).number().longValue();
        }
    }
}