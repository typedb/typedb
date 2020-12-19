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
import grakn.core.concept.type.AttributeType;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.procedure.GraphProcedure;
import grakn.core.traversal.procedure.ProcedureVertex;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlInsert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static grakn.core.common.parameters.Arguments.Session.Type.DATA;
import static grakn.core.common.parameters.Arguments.Session.Type.SCHEMA;
import static grakn.core.common.parameters.Arguments.Transaction.Type.READ;
import static grakn.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static graql.lang.Graql.parseQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TraversalTest2 {

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
                final String queryString = "define\n" +
                        "sale sub relation,\n" +
                        "   relates buyer,\n" +
                        "   relates seller;\n" +
                        "person sub entity,\n" +
                        "   owns name," +
                        "   plays sale:buyer,\n" +
                        "   plays sale:seller;\n" +
                        "name sub attribute, value string;";
                final GraqlDefine query = parseQuery(queryString);
                transaction.query().define(query);
                transaction.commit();
            }
        }

        try (RocksSession session = grakn.session(database, DATA)) {
            try (RocksTransaction transaction = session.transaction(WRITE)) {
                final String queryString = "insert\n" +
                        "      $a isa person, has name 'alice';\n" +
                        "      $b isa person, has name 'bob';\n" +
                        "      $c isa person, has name 'charlie';\n" +
                        "      (buyer: $a, seller: $b) isa sale;\n" +
                        "      (buyer: $b, seller: $c) isa sale;";
                final GraqlInsert query = parseQuery(queryString);
                transaction.query().insert(query);
                transaction.commit();
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
    public void test_traversal_repeat() {
        int success = 0, fail = 0;
        for (int i = 0; i < 20; i++) {
            try {
                test_traversal();
                success++;
                System.out.println("SUCCESS --------------");
            } catch (AssertionError e) {
                fail++;
                System.out.println("FAIL -----------------");
            }
        }
        System.out.println(String.format("Success: %s, Fail: %s", success, fail));
    }

    @Test
    public void test_traversal() {
        try (RocksTransaction transaction = session.transaction(READ)) {
            final String queryString = "match\n" +
                    "        (buyer: $a, seller: $b) isa sale;\n" +
                    "        (buyer: $b, seller: $c) isa sale;";
            ResourceIterator<ConceptMap> answers = transaction.query().match(parseQuery(queryString).asMatch(), false);
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

    @Test
    public void test_traversal_procedure() {
        try (RocksTransaction transaction = session.transaction(READ)) {
            GraphProcedure.Builder proc = GraphProcedure.builder(14);
            Traversal.Parameters params = new Traversal.Parameters();

            ProcedureVertex.Type _sale = proc.labelledType("sale", true);
            ProcedureVertex.Type _sale_buyer = proc.labelledType("sale:buyer");
            ProcedureVertex.Type _sale_seller = proc.labelledType("sale:seller");

            proc.setLabel(_sale, "sale");
            proc.setLabel(_sale_buyer, "buyer", "sale");
            proc.setLabel(_sale_seller, "seller", "sale");

            ProcedureVertex.Thing a = proc.namedThing("a");
            ProcedureVertex.Thing b = proc.namedThing("b");
            ProcedureVertex.Thing c = proc.namedThing("c");

            ProcedureVertex.Thing _0 = proc.anonymousThing(0);
            ProcedureVertex.Thing _0_0 = proc.scopedThing(_0, 0);
            ProcedureVertex.Thing _0_1 = proc.scopedThing(_0, 1);
            ProcedureVertex.Thing _1 = proc.anonymousThing(1);
            ProcedureVertex.Thing _1_0 = proc.scopedThing(_1, 0);
            ProcedureVertex.Thing _1_1 = proc.scopedThing(_1, 1);

            proc.backwardIsa(1, _sale, _1, true);
            proc.forwardRelating(2, _1, _1_0);
            proc.backwardPlaying(3, _1_0, b);
            proc.forwardRelating(4, _1, _1_1);
            proc.forwardPlaying(5, b, _0_1);
            proc.backwardPlaying(6, _1_1, c);
            proc.forwardIsa(7, _1_0, _sale_buyer, true);
            proc.backwardIsa(8, _sale, _0, true);
            proc.forwardRelating(9, _0, _0_0);
            proc.forwardRelating(10, _0, _0_1);
            proc.forwardIsa(11, _1_1, _sale_seller, true);
            proc.forwardIsa(12, _0_0, _sale_buyer, true);
            proc.backwardIsa(13, _sale_seller, _0_1, true);
            proc.backwardPlaying(14, _0_0, a);

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
