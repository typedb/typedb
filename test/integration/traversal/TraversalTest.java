/*
 * Copyright (C) 3030 Grakn Labs
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
 *
 */

package grakn.core.traversal;

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Arguments;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.type.AttributeType;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlMatch;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

import static grakn.core.common.parameters.Arguments.Session.Type.DATA;
import static grakn.core.test.integration.util.Util.assertNotNulls;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TraversalTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("traversal-test");
    private static String database = "traversal-test";

    private static RocksGrakn grakn;
    private static RocksSession session;

    @BeforeClass
    public static void before() throws IOException {
        Util.resetDirectory(directory);
        grakn = RocksGrakn.open(directory);
        grakn.databases().create(database);

        try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                final String queryString = "define " +
                        "name sub attribute, value string; " +
                        "age sub attribute, value long; " +
                        "person sub entity, owns name, owns age, plays friendship:friend; " +
                        "friendship sub relation, relates friend; ";
                final GraqlDefine query = Graql.parseQuery(queryString);
                transaction.query().define(query);
                transaction.commit();
            }
        }

        try (RocksSession session = grakn.session(database, DATA)) {
            try (RocksTransaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                final String queryString = "insert " +
                        "$a isa person, has name 'alice', has age 25; " +
                        "$b isa person, has name 'bob', has age 26; " +
                        "$c isa person, has name 'charlie', has age 20; " +
                        "($a, $b) isa friendship; " +
                        "($b, $c) isa friendship; " +
                        "($a, $c) isa friendship;";

                final GraqlInsert query = Graql.parseQuery(queryString);
                transaction.query().insert(query);
                transaction.commit();
            }
            try (RocksTransaction transaction = session.transaction(Arguments.Transaction.Type.READ)) {
                assertEquals(3, transaction.concepts().getEntityType("person").getInstances().count());
                assertEquals(3, transaction.concepts().getRelationType("friendship").getInstances().count());
                assertEquals(3, transaction.concepts().getAttributeType("name").getInstances().count());
                assertEquals(3, transaction.concepts().getAttributeType("age").getInstances().count());
            }
        }

        session = grakn.session(database, DATA);
    }

    @AfterClass
    public static void after() {
        session.close();
        grakn.close();
    }

    @Test
    public void traversal_1_repeat() {
        int success = 0, fail = 0;
        for (int i = 0; i < 30; i++) {
            try {
                traversal_1();
                success++;
            } catch (AssertionError e) {
                fail++;
            }
        }
        System.out.println(String.format("Success: %s, Fail: %s", success, fail));
    }

    //    @Test
    public void traversal_1() {
        try (RocksTransaction transaction = session.transaction(Arguments.Transaction.Type.READ)) {

            System.out.println("------------");
            final String queryString = "match $x isa person, has name 'alice'; " +
                    "$y isa person, has age > 21;" +
                    "(friend: $x, friend: $y) isa friendship; ";
            final GraqlMatch query = Graql.parseQuery(queryString);
            Instant start = Instant.now();
            ResourceIterator<ConceptMap> answers = transaction.query().match(query);
            assertNotNulls(answers);
            assertTrue(answers.hasNext());
            ConceptMap answer = answers.next();

            Instant first = Instant.now();
            System.out.println(String.format("Duration for first answer: %s (ms)", Duration.between(start, first).toMillis()));

            AttributeType.String name = transaction.concepts().getAttributeType("name").asString();
            assertTrue(answer.get("x").asThing().getHas(name).anyMatch(n -> n.getValue().equals("alice")));
            assertTrue(answer.get("y").asThing().getHas(name).anyMatch(n -> n.getValue().equals("bob")));

            System.out.println(answer.get("x").asThing().getHas(name).findFirst().get().getValue());
            System.out.println(answer.get("y").asThing().getHas(name).findFirst().get().getValue());
            for (int i = 0; i < 10 && answers.hasNext(); i++) {
                answer = answers.next();
                System.out.println(answer.get("x").asThing().getHas(name).findFirst().get().getValue());
                System.out.println(answer.get("y").asThing().getHas(name).findFirst().get().getValue());
            }
            System.out.println("------------");


            Instant finish = Instant.now();
            System.out.println(String.format("Duration for all answers: %s (ms)", Duration.between(start, finish).toMillis()));
        }
    }

    @Test
    public void traversal_2_repeat() {
        int success = 0, fail = 0;
        for (int i = 0; i < 30; i++) {
            try {
                traversal_2();
                success++;
            } catch (AssertionError e) {
                fail++;
            }
        }
        System.out.println(String.format("Success: %s, Fail: %s", success, fail));
    }

    //    @Test
    public void traversal_2() {
        try (RocksTransaction transaction = session.transaction(Arguments.Transaction.Type.READ)) {

            System.out.println("------------");
            final String queryString = "match $x isa person, has name 'alice'; " +
                    "$y isa person, has name 'charlie';" +
                    "(friend: $x, friend: $y) isa friendship; ";
            final GraqlMatch query = Graql.parseQuery(queryString);
            Instant start = Instant.now();
            ResourceIterator<ConceptMap> answers = transaction.query().match(query);
            assertNotNulls(answers);
            assertTrue(answers.hasNext());
            ConceptMap answer = answers.next();

            Instant first = Instant.now();
            System.out.println(String.format("Duration for first answer: %s (ms)", Duration.between(start, first).toMillis()));

            AttributeType.String name = transaction.concepts().getAttributeType("name").asString();
            assertTrue(answer.get("x").asThing().getHas(name).anyMatch(n -> n.getValue().equals("alice")));
            assertTrue(answer.get("y").asThing().getHas(name).anyMatch(n -> n.getValue().equals("charlie")));

            System.out.println(answer.get("x").asThing().getHas(name).findFirst().get().getValue());
            System.out.println(answer.get("y").asThing().getHas(name).findFirst().get().getValue());
            for (int i = 0; i < 10 && answers.hasNext(); i++) {
                answer = answers.next();
                System.out.println(answer.get("x").asThing().getHas(name).findFirst().get().getValue());
                System.out.println(answer.get("y").asThing().getHas(name).findFirst().get().getValue());
            }
            System.out.println("------------");


            Instant finish = Instant.now();
            System.out.println(String.format("Duration for all answers: %s (ms)", Duration.between(start, finish).toMillis()));
        }
    }

    @Test
    public void traversal_3_repeat() {
        int success = 0, fail = 0;
        for (int i = 0; i < 30; i++) {
            try {
                traversal_3();
                success++;
            } catch (AssertionError e) {
                fail++;
            }
        }
        System.out.println(String.format("Success: %s, Fail: %s", success, fail));
    }

    //    @Test
    public void traversal_3() {
        try (RocksTransaction transaction = session.transaction(Arguments.Transaction.Type.READ)) {

            System.out.println("------------");
            final String queryString = "match $x isa person, has name 'bob'; " +
                    "$y isa person, has name 'charlie';" +
                    "(friend: $x, friend: $y) isa friendship; ";
            final GraqlMatch query = Graql.parseQuery(queryString);
            Instant start = Instant.now();
            ResourceIterator<ConceptMap> answers = transaction.query().match(query);
            assertNotNulls(answers);
            assertTrue(answers.hasNext());
            ConceptMap answer = answers.next();

            Instant first = Instant.now();
            System.out.println(String.format("Duration for first answer: %s (ms)", Duration.between(start, first).toMillis()));

            AttributeType.String name = transaction.concepts().getAttributeType("name").asString();
            assertTrue(answer.get("x").asThing().getHas(name).anyMatch(n -> n.getValue().equals("bob")));
            assertTrue(answer.get("y").asThing().getHas(name).anyMatch(n -> n.getValue().equals("charlie")));

            System.out.println(answer.get("x").asThing().getHas(name).findFirst().get().getValue());
            System.out.println(answer.get("y").asThing().getHas(name).findFirst().get().getValue());
            for (int i = 0; i < 10 && answers.hasNext(); i++) {
                answer = answers.next();
                System.out.println(answer.get("x").asThing().getHas(name).findFirst().get().getValue());
                System.out.println(answer.get("y").asThing().getHas(name).findFirst().get().getValue());
            }
            System.out.println("------------");


            Instant finish = Instant.now();
            System.out.println(String.format("Duration for all answers: %s (ms)", Duration.between(start, finish).toMillis()));
        }
    }

    @Test
    public void traversal_4_repeat() {
        int success = 0, fail = 0;
        for (int i = 0; i < 30; i++) {
            try {
                traversal_4();
                success++;
            } catch (AssertionError e) {
                fail++;
            }
        }
        System.out.println(String.format("Success: %s, Fail: %s", success, fail));
    }

    //    @Test
    public void traversal_4() {
        try (RocksTransaction transaction = session.transaction(Arguments.Transaction.Type.READ)) {

            System.out.println("------------");
            final String queryString = "match $x isa person, has age 20; " +
                    "$y isa person, has age 25;" +
                    "(friend: $x, friend: $y) isa friendship; ";
            final GraqlMatch query = Graql.parseQuery(queryString);
            Instant start = Instant.now();
            ResourceIterator<ConceptMap> answers = transaction.query().match(query);
            assertNotNulls(answers);
            assertTrue(answers.hasNext());
            ConceptMap answer = answers.next();

            Instant first = Instant.now();
            System.out.println(String.format("Duration for first answer: %s (ms)", Duration.between(start, first).toMillis()));

            AttributeType.Long age = transaction.concepts().getAttributeType("age").asLong();
            assertTrue(answer.get("x").asThing().getHas(age).anyMatch(n -> n.getValue().equals(20L)));
            assertTrue(answer.get("y").asThing().getHas(age).anyMatch(n -> n.getValue().equals(25L)));

            System.out.println(answer.get("x").asThing().getHas(age).findFirst().get().getValue());
            System.out.println(answer.get("y").asThing().getHas(age).findFirst().get().getValue());
            for (int i = 0; i < 10 && answers.hasNext(); i++) {
                answer = answers.next();
                System.out.println(answer.get("x").asThing().getHas(age).findFirst().get().getValue());
                System.out.println(answer.get("y").asThing().getHas(age).findFirst().get().getValue());
            }
            System.out.println("------------");


            Instant finish = Instant.now();
            System.out.println(String.format("Duration for all answers: %s (ms)", Duration.between(start, finish).toMillis()));
        }
    }

    @Test
    public void traversal_5_repeat() {
        int success = 0, fail = 0;
        for (int i = 0; i < 30; i++) {
            try {
                traversal_5();
                success++;
            } catch (AssertionError e) {
                fail++;
            }
        }
        System.out.println(String.format("Success: %s, Fail: %s", success, fail));
    }

    //    @Test
    public void traversal_5() {
        try (RocksTransaction transaction = session.transaction(Arguments.Transaction.Type.READ)) {

            System.out.println("------------");
            final String queryString = "match $x isa person, has age 20; " +
                    "$y isa person, has age > 21;" +
                    "(friend: $x, friend: $y) isa friendship; ";
            final GraqlMatch query = Graql.parseQuery(queryString);

            int count = 0;
            Instant startTotal = Instant.now();
            Instant startIter = startTotal;
            ResourceIterator<ConceptMap> answers = transaction.query().match(query);
            assertNotNulls(answers);
            assertTrue(answers.hasNext());
            Instant endFirst = Instant.now();
            do {
                count++;
                ConceptMap answer = answers.next();
                Instant endNext = Instant.now();
                AttributeType.Long age = transaction.concepts().getAttributeType("age").asLong();

                assertTrue(answer.get("x").asThing().getHas(age).anyMatch(n -> n.getValue().equals(30L)));
                assertTrue(answer.get("y").asThing().getHas(age).anyMatch(n -> n.getValue() > 21));

                System.out.println(answer.get("x").asThing().getHas(age).findFirst().get().getValue());
                System.out.println(answer.get("y").asThing().getHas(age).findFirst().get().getValue());
                System.out.println("------------");
                System.out.println(String.format("Duration for answer #%s: %s (ms)", count, Duration.between(startIter, endNext).toMillis()));
                System.out.println("------------");
                startIter = Instant.now();
            } while (answers.hasNext());
            Instant endTotal = startIter;
            System.out.println(String.format("Duration of the first answer            : %s (ms)", Duration.between(startTotal, endFirst).toMillis()));
            if (count > 1)
                System.out.println(String.format("Average duration of every 'next' answer : %s (ms)", Duration.between(endFirst, endTotal).toMillis() / (count - 1)));
            System.out.println(String.format("Duration for all answers                : %s (ms)", Duration.between(startTotal, endTotal).toMillis()));
            System.out.println("------------");
        }
    }

    @Test
    public void traversal_6_repeat() {
        int success = 0, fail = 0;
        for (int i = 0; i < 30; i++) {
            try {
                traversal_6();
                success++;
            } catch (AssertionError e) {
                fail++;
            }
        }
        System.out.println(String.format("Success: %s, Fail: %s", success, fail));
    }

    @Test
    public void traversal_6() {
        try (RocksTransaction transaction = session.transaction(Arguments.Transaction.Type.READ)) {

            System.out.println("------------");
            final String str1 = "match $x isa person, has age 26; " +
                    "$y isa person, has age 20;" +
                    "(friend: $x, friend: $y) isa friendship; ";
            final GraqlMatch query1 = Graql.parseQuery(str1);

            ResourceIterator<ConceptMap> answers1 = transaction.query().match(query1);
            assertNotNulls(answers1);
            answers1.hasNext();

            System.out.println("------------");
            final String str2 = "match $x isa person, has age > 25; " +
                    "$y isa person, has age < 22;" +
                    "(friend: $x, friend: $y) isa friendship; ";
            final GraqlMatch query2 = Graql.parseQuery(str2);

            ResourceIterator<ConceptMap> answers2 = transaction.query().match(query2);
            assertNotNulls(answers2);
            answers2.hasNext();

        }
    }
}
