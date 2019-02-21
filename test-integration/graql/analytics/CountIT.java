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

import grakn.core.graql.answer.Numeric;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Entity;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.internal.Schema;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("CheckReturnValue")
public class CountIT {

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
    public void testCountAfterCommit() {
        String nameThing = "thingy";
        String nameAnotherThing = "another";

        // assert the tx is empty
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            assertEquals(0, tx.execute(Graql.compute().count()).get(0).number().intValue());
            assertEquals(0, tx.execute(Graql.compute().count().attributes(true)).get(0).number().intValue());
        }

        // add 2 instances
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            EntityType thingy = tx.putEntityType(nameThing);
            thingy.create();
            thingy.create();
            tx.commit();
        }

        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            assertEquals(2, tx.execute(Graql.compute().count().in(nameThing)).get(0).number().intValue());
        }

        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            EntityType anotherThing = tx.putEntityType(nameAnotherThing);
            anotherThing.create();
            tx.commit();
        }

        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            // assert computer returns the correct count of instances
            assertEquals(2, tx.execute(Graql.compute().count().in(nameThing).attributes(true)).get(0).number().intValue());
            assertEquals(3, tx.execute(Graql.compute().count()).get(0).number().intValue());
        }
    }

    @Test
    public void testConcurrentCount() {
        String nameThing = "thingy";
        String nameAnotherThing = "another";

        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
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
        // collecting the result in the end so server won't stop before the test finishes
        Set<Long> result;
        result = list.parallelStream()
                .map(i -> executeCount(session))
                .collect(Collectors.toSet());
        assertEquals(1, result.size());
        assertEquals(3L, result.iterator().next().longValue());

        result = list.parallelStream()
                .map(i -> executeCount(session))
                .collect(Collectors.toSet());
        assertEquals(1, result.size());
        assertEquals(3L, result.iterator().next().longValue());
    }

    @Test
    public void testHasResourceEdges() {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            EntityType person = tx.putEntityType("person");
            AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
            person.has(name);
            Entity aPerson = person.create();
            aPerson.has(name.create("jason"));
            tx.commit();
        }

        Numeric count;
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            count = tx.execute(Graql.compute().count()).get(0);
            assertEquals(1, count.number().intValue());

            count = tx.execute(Graql.compute().count().attributes(true)).get(0);
            assertEquals(3L, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("name")).get(0);
            assertEquals(1, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("@has-name")).get(0);
            assertEquals(1, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("@has-name", "thing")).get(0);
            assertEquals(3, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("@has-name", "name")).get(0);
            assertEquals(2, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("relationship")).get(0);
            assertEquals(0, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("relationship").attributes(true)).get(0);
            assertEquals(1, count.number().intValue());
        }

        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {

            // manually construct the relation type and instance
            EntityType person = tx.getEntityType("person");
            Entity aPerson = person.create();
            AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
            Attribute jason = name.create("jason");

            Role resourceOwner = tx.putRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of("name")));
            person.plays(resourceOwner);
            Role resourceValue = tx.putRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of("name")));
            name.plays(resourceValue);

            RelationType relationshipType =
                    tx.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of("name")))
                            .relates(resourceOwner).relates(resourceValue);
            relationshipType.create()
                    .assign(resourceOwner, aPerson)
                    .assign(resourceValue, jason);
            tx.commit();
        }

        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            count = tx.execute(Graql.compute().count()).get(0);
            assertEquals(2, count.number().intValue());

            count = tx.execute(Graql.compute().count().attributes(true)).get(0);
            assertEquals(5, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("name")).get(0);
            assertEquals(1, count.number().intValue());

            count = tx.execute(Graql.compute().count().attributes(true).in("name")).get(0);
            assertEquals(1, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("@has-name")).get(0);
            assertEquals(2, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("@has-name", "thing")).get(0);
            assertEquals(5, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("@has-name", "name")).get(0);
            assertEquals(3, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("relationship")).get(0);
            assertEquals(0, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("relationship").attributes(true)).get(0);
            assertEquals(2, count.number().intValue());
        }
    }

    @Test
    public void testHasResourceVerticesAndEdges() {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {

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

            RelationType relationshipType =
                    tx.putRelationType(Schema.ImplicitType.HAS.getLabel(Label.of("name")))
                            .relates(resourceOwner).relates(resourceValue);
            // here relationship type is still implicit
            relationshipType.create()
                    .assign(resourceOwner, aPerson)
                    .assign(resourceValue, jason);

            tx.commit();
        }

        Numeric count;
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            count = tx.execute(Graql.compute().count()).get(0);
            assertEquals(1, count.number().intValue());

            count = tx.execute(Graql.compute().count().attributes(true)).get(0);
            assertEquals(3, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("name")).get(0);
            assertEquals(1, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("@has-name")).get(0);
            assertEquals(1, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("@has-name", "name")).get(0);
            assertEquals(2, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("relationship")).get(0);
            assertEquals(0, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("relationship").attributes(true)).get(0);
            assertEquals(1, count.number().intValue());
        }

        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            EntityType person = tx.getEntityType("person");
            AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
            Entity aPerson = person.create();
            aPerson.has(name.attribute("jason"));
            tx.commit();
        }

        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            count = tx.execute(Graql.compute().count().attributes(true)).get(0);
            assertEquals(5, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("name")).get(0);
            assertEquals(1, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("@has-name")).get(0);
            assertEquals(2, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("@has-name", "name")).get(0);
            assertEquals(3, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("relationship")).get(0);
            assertEquals(0, count.number().intValue());

            count = tx.execute(Graql.compute().count().in("relationship").attributes(true)).get(0);
            assertEquals(2, count.number().intValue());
        }
    }

    private Long executeCount(Session session) {
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            return tx.execute(Graql.compute().count()).get(0).number().longValue();
        }
    }
}