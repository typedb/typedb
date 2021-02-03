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
 *
 */

package grakn.core.traversal;

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.RelationType;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.procedure.GraphProcedure;
import grakn.core.traversal.procedure.ProcedureVertex;
import graql.lang.query.GraqlDefine;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static grakn.core.common.parameters.Arguments.Session.Type.DATA;
import static grakn.core.common.parameters.Arguments.Session.Type.SCHEMA;
import static grakn.core.common.parameters.Arguments.Transaction.Type.READ;
import static grakn.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static graql.lang.Graql.parseQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TraversalTest5 {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("traversal-test-2");
    private static String database = "traversal-test-2";

    private static RocksGrakn grakn;
    private static RocksSession session;

    @BeforeClass
    public static void before() throws IOException {
        Util.resetDirectory(directory);
        grakn = RocksGrakn.open(directory);
        grakn.databases().create(database);

        try (RocksSession session = grakn.session(database, SCHEMA)) {
            try (RocksTransaction transaction = session.transaction(WRITE)) {
                String queryString = "define\n" +
                        "animal sub entity;\n" +
                        "bird sub animal, plays FLYS:IS_FLYS, plays WALKS:IS_WALKS;\n" +
                        "dog sub animal, plays WALKS:IS_WALKS;\n" +
                        "\n" +
                        "FLYS sub relation,\n" +
                        "    relates IS_FLYS;\n" +
                        "WALKS sub relation,\n" +
                        "    relates IS_WALKS;";
                GraqlDefine query = parseQuery(queryString);
                transaction.query().define(query);
                transaction.commit();
            }
        }

//        try (RocksSession session = grakn.session(database, DATA)) {
//            try (RocksTransaction transaction = session.transaction(WRITE)) {
//                String insertStr = "insert\n" +
//                        "      $c1 isa company, has name \"Apple\";\n" +
//                        "      $c2 isa company, has name \"Google\";\n" +
//                        "      $p1 isa person, has name \"Elena\";\n" +
//                        "      $p2 isa person, has name \"Flynn\";\n" +
//                        "      $p3 isa person, has name \"Lyudmila\";\n" +
//                        "      $e1 (employer: $c1, employee: $p1, employee: $p2) isa employment;\n" +
//                        "      $e2 (employer: $c2, employee: $p3) isa employment;";
//                transaction.query().insert(parseQuery(insertStr).asInsert());
//                transaction.commit();
//            }
//        }

        session = grakn.session(database, DATA);
    }

    @AfterClass
    public static void after() {
        session.close();
        grakn.close();
    }

    @Test
    public void test_negation_2() {
        try (RocksTransaction tx = session.transaction(WRITE)) {
            tx.query().insert(parseQuery(
                    "insert " +
                            "$b isa bird; ($b) isa FLYS; ($b) isa WALKS;\n" +
                            "$d isa dog; ($d) isa WALKS;"));
            tx.commit();
        }
        try (RocksTransaction tx = session.transaction(READ)) {
            System.out.println(tx.query().match(parseQuery(
                    "match $a isa animal; ($a) isa WALKS; not { ($a) isa FLYS; };"
            ).asMatch()).toList());
        }
    }

    @Test
    public void test_predicate() {
        try (RocksTransaction tx = session.transaction(WRITE)) {
            tx.query().insert(parseQuery(
                    "insert " +
                            "$b isa bird;\n" +
                            "$d isa dog;"));
            tx.commit();
        }
        try (RocksTransaction tx = session.transaction(READ)) {
            System.out.println(tx.query().match(parseQuery(
                    "match $b isa bird; $d isa dog; $b > $d;"
            ).asMatch()).toList());
        }
    }


    @Test
    public void test_negation() {
        System.out.println();
        try (RocksTransaction transaction = session.transaction(READ)) {
            AttributeType.String name = transaction.concepts().getAttributeType("name").asString();
            ResourceIterator<ConceptMap> answers;
            List<ConceptMap> answerList;

            answers = transaction.query().match(parseQuery(
                    "match\n"
                            + "$x isa company;\n"
                            + "$y isa person;\n"
                            + "$z isa person;\n"
                            + "not { $y is $z; };\n"
                            + "($x, $y) isa relation;\n"
                            + "get $x, $y, $z;"
            ).asMatch());
            assertTrue(answers.hasNext());
            answerList = answers.toList();
            printAnswer(name, answerList);
            assertEquals(6, answerList.size());
        }
    }

    @Test
    public void test_mean() {
        System.out.println();
        try (RocksTransaction transaction = session.transaction(READ)) {
            ResourceIterator<ConceptMap> answers = transaction.query().match(parseQuery("match $x isa person, has age $a;").asMatch());
            assertEquals(3, answers.count());
            Numeric answer = transaction.query().match(parseQuery("match $x isa person, has age $a; mean $a;").asMatchAggregate());
            assertEquals(40.0, answer.asDouble(), 0.001);
        }
    }

    @Test
    public void test_relation() {
        System.out.println();
        try (RocksTransaction transaction = session.transaction(READ)) {
            ResourceIterator<ConceptMap> answers = transaction.query().match(parseQuery("match (friend: $x, friend: $x);").asMatch());
            assertTrue(answers.hasNext());

            RelationType friendship = transaction.concepts().getRelationType("friendship");
            Relation f = friendship.getInstances().findFirst().get();

            List<? extends Thing> pl = f.getPlayers().collect(Collectors.toList());
            pl.forEach(p -> System.out.println(p.getIIDForPrinting()));
        }
    }

//    @Test
//    public void test_traversal_repeat() {
//        int success = 0, fail = 0;
//        for (int i = 0; i < 20; i++) {
//            try {
//                test_traversal();
//                success++;
//                System.out.println("SUCCESS --------------");
//            } catch (AssertionError e) {
//                fail++;
//                System.out.println("FAIL -----------------");
//            }
//        }
//        System.out.println(String.format("Success: %s, Fail: %s", success, fail));
//    }

    @Test
    public void test_traversal() {
        try (RocksTransaction transaction = session.transaction(READ)) {
            AttributeType.String name = transaction.concepts().getAttributeType("name").asString();
            ResourceIterator<ConceptMap> answers;
            List<ConceptMap> answerList;

            answers = transaction.query().match(parseQuery("match $m (wife: $x, husband: $y) isa hetero-marriage;").asMatch());
            assertTrue(answers.hasNext());
            answerList = answers.toList();
            printAnswer(name, answerList);
            assertEquals(1, answerList.size());

            try {
                transaction.query().match(parseQuery("match $m (wife: $x, husband: $y) isa civil-marriage;").asMatch()).toList();
                fail();
            } catch (Throwable e) {
                assertTrue(true);
            }

            answers = transaction.query().match(parseQuery("match $m (wife: $x, husband: $y) isa marriage;").asMatch());
            assertTrue(answers.hasNext());
            answerList = answers.toList();
            printAnswer(name, answerList);
            assertEquals(1, answerList.size());

            answers = transaction.query().match(parseQuery("match $m (wife: $x, husband: $y) isa relation;").asMatch());
            assertTrue(answers.hasNext());
            answerList = answers.toList();
            printAnswer(name, answerList);
            assertEquals(1, answerList.size());

            answers = transaction.query().match(parseQuery("match $m (spouse: $x, spouse: $y) isa hetero-marriage;").asMatch());
            assertTrue(answers.hasNext());
            answerList = answers.toList();
            printAnswer(name, answerList);
            assertEquals(2, answerList.size());

            answers = transaction.query().match(parseQuery("match $m (spouse: $x, spouse: $y) isa civil-marriage;").asMatch());
            assertTrue(answers.hasNext());
            answerList = answers.toList();
            printAnswer(name, answerList);
            assertEquals(2, answerList.size());

            answers = transaction.query().match(parseQuery("match $m (spouse: $x, spouse: $y) isa marriage;").asMatch());
            assertTrue(answers.hasNext());
            answerList = answers.toList();
            printAnswer(name, answerList);
            assertEquals(4, answerList.size());

            answers = transaction.query().match(parseQuery("match $m (spouse: $x, spouse: $y) isa relation;").asMatch());
            assertTrue(answers.hasNext());
            answerList = answers.toList();
            printAnswer(name, answerList);
            assertEquals(4, answerList.size());

            answers = transaction.query().match(parseQuery("match $m (role: $x, role: $y) isa hetero-marriage;").asMatch());
            assertTrue(answers.hasNext());
            answerList = answers.toList();
            printAnswer(name, answerList);
            assertEquals(2, answerList.size());

            answers = transaction.query().match(parseQuery("match $m (role: $x, role: $y) isa civil-marriage;").asMatch());
            assertTrue(answers.hasNext());
            answerList = answers.toList();
            printAnswer(name, answerList);
            assertEquals(2, answerList.size());

            answers = transaction.query().match(parseQuery("match $m (role: $x, role: $y) isa marriage;").asMatch());
            assertTrue(answers.hasNext());
            answerList = answers.toList();
            printAnswer(name, answerList);
            assertEquals(4, answerList.size());

            answers = transaction.query().match(parseQuery("match $m (role: $x, role: $y) isa relation;").asMatch());
            assertTrue(answers.hasNext());
            answerList = answers.toList();
            printAnswer(name, answerList);
            assertEquals(4, answerList.size());
        }
    }

    public void printAnswer(AttributeType.String name, List<ConceptMap> answers) {
        for (ConceptMap answer : answers) {
            System.out.println("x: " + answer.get("x").asThing().getHas(name).findFirst().get().getValue());
            System.out.println("y: " + answer.get("y").asThing().getHas(name).findFirst().get().getValue());
            System.out.println("z: " + answer.get("z").asThing().getHas(name).findFirst().get().getValue());
            System.out.println("-------------");
        }
    }

    @Test
    public void test_traversal_procedure() {
        try (RocksTransaction transaction = session.transaction(READ)) {
            GraphProcedure.Builder proc = GraphProcedure.builder(14);
            Traversal.Parameters params = new Traversal.Parameters();

            ProcedureVertex.Type _sale = proc.labelledType("sale", true);
            ProcedureVertex.Type _sale_buyer = proc.labelledType("sale:buyer");
            ProcedureVertex.Type _sale_seller = proc.labelledType("sale:seller");

            proc.setLabel(_sale, "sale");
            proc.setLabel(_sale_buyer, "sale:buyer");
            proc.setLabel(_sale_seller, "sale:seller");

            ProcedureVertex.Thing a = proc.namedThing("a");
            ProcedureVertex.Thing b = proc.namedThing("b");
            ProcedureVertex.Thing c = proc.namedThing("c");

            ProcedureVertex.Thing _0 = proc.anonymousThing(0);
            ProcedureVertex.Thing _0_buyer = proc.scopedThing(_0, _sale_buyer, a, 1);
            ProcedureVertex.Thing _0_seller = proc.scopedThing(_0, _sale_seller, b, 1);
            ProcedureVertex.Thing _1 = proc.anonymousThing(1);
            ProcedureVertex.Thing _1_buyer = proc.scopedThing(_1, _sale_buyer, b, 1);
            ProcedureVertex.Thing _1_seller = proc.scopedThing(_1, _sale_seller, c, 1);

            proc.backwardIsa(1, _sale, _1, true);
            proc.forwardRelating(2, _1, _1_buyer);
            proc.backwardPlaying(3, _1_buyer, b);
            proc.forwardRelating(4, _1, _1_seller);
            proc.forwardPlaying(5, b, _0_seller);
            proc.backwardPlaying(6, _1_seller, c);
            proc.forwardIsa(7, _1_buyer, _sale_buyer, true);
            proc.backwardIsa(8, _sale, _0, true);
            proc.forwardRelating(9, _0, _0_buyer);
            proc.forwardRelating(10, _0, _0_seller);
            proc.forwardIsa(11, _1_seller, _sale_seller, true);
            proc.forwardIsa(12, _0_buyer, _sale_buyer, true);
            proc.backwardIsa(13, _sale_seller, _0_seller, true);
            proc.backwardPlaying(14, _0_buyer, a);

            ResourceIterator<VertexMap> vertices = transaction.traversal().iterator(proc.build(), params);
            assertTrue(vertices.hasNext());
            ResourceIterator<ConceptMap> answers = transaction.concepts().conceptMaps(vertices);
            assertTrue(answers.hasNext());
            ConceptMap answer;
            AttributeType.String name = transaction.concepts().getAttributeType("name").asString();
            int count = 0;
            while (answers.hasNext()) {
                count++;
                answer = answers.next();
                System.out.println(answer.get("a").asThing().getHas(name).findFirst().get().getValue());
                System.out.println(answer.get("b").asThing().getHas(name).findFirst().get().getValue());
                System.out.println(answer.get("c").asThing().getHas(name).findFirst().get().getValue());
            }
            assertEquals(1, count);
        }
    }
}
