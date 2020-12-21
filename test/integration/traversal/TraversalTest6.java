/*
 * Copyright (C) 2020 Grakn Labs
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.traversal;

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import graql.lang.query.GraqlDefine;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.parameters.Arguments.Session.Type.DATA;
import static grakn.core.common.parameters.Arguments.Session.Type.SCHEMA;
import static grakn.core.common.parameters.Arguments.Transaction.Type.READ;
import static grakn.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static grakn.core.test.integration.util.Util.assertNotNulls;
import static graql.lang.Graql.parseQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TraversalTest6 {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("traversal-test-6");
    private static String database = "traversal-test-6";

    private static RocksGrakn grakn;
    private static RocksSession session;

    @BeforeClass
    public static void before() throws IOException {
        Util.resetDirectory(directory);
        grakn = RocksGrakn.open(directory);
        grakn.databases().create(database);

        try (RocksSession session = grakn.session(database, SCHEMA)) {
            try (RocksTransaction transaction = session.transaction(WRITE)) {
                final String queryString =
                        "define " +
                                "animal sub entity; " +
                                "mammal sub animal; " +
                                "reptile sub animal; " +
                                "tortoise sub reptile; " +
                                "person sub mammal, owns name, owns email, plays marriage:spouse; " +
                                "man sub person, plays marriage:husband; " +
                                "woman sub person, plays marriage:wife; " +
                                "dog sub mammal, owns name, owns tail-length;" +
                                "rat sub mammal, owns tail-length; " +
                                "name sub attribute, value string; " +
                                "email sub attribute, value string; " +
                                "tail-length sub attribute, value long; " +
                                "marriage sub relation, relates husband, relates wife, relates spouse;" +
                                "nickname sub attribute, value string, owns surname, owns middlename;" +
                                "surname sub attribute, value string, owns nickname;" +
                                "middlename sub attribute, value string, owns firstname;" +
                                "firstname sub attribute, value string, owns surname;";
                final GraqlDefine query = parseQuery(queryString);
                transaction.query().define(query);
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

    private Map<String, Set<String>> retrieveAnswers(ResourceIterator<ConceptMap> answers) {
        Map<String, Set<String>> res = new HashMap<>();
        while (answers.hasNext()) {
            answers.next().concepts().forEach((k, v) -> {
                res.putIfAbsent(k.name(), new HashSet<>());
                assertTrue(v.isType());
                res.get(k.name()).add(v.asType().getLabel().scopedName());
            });
        }
        return res;
    }

    @Test
    public void test_plays_inheritance() {
        try (RocksTransaction transaction = session.transaction(READ)) {
            final String queryString = "match $p plays marriage:spouse;";
            ResourceIterator<ConceptMap> answers = transaction.query().match(parseQuery(queryString).asMatch(), false);
            assertNotNulls(answers);
            assertTrue(answers.hasNext());
            Map<String, Set<String>> result = retrieveAnswers(answers);
            assertEquals(1, result.keySet().size());

            Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
                put("p", set("person", "man", "woman"));
            }};

            assertEquals(expected, result);
        }
    }

    @Test
    public void test_owns_cycle() {
        try (RocksTransaction transaction = session.transaction(READ)) {
            final String queryString = "match $a sub attribute, owns $b; $b sub attribute, owns $a;";
            ResourceIterator<ConceptMap> answers = transaction.query().match(parseQuery(queryString).asMatch(), false);
            assertNotNulls(answers);
            assertTrue(answers.hasNext());
            Map<String, Set<String>> result = retrieveAnswers(answers);
            assertEquals(2, result.keySet().size());

            Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
                put("a", set("nickname", "surname"));
                put("b", set("nickname", "surname"));
            }};

            assertEquals(expected, result);
        }
    }

    @Test
    public void test_owns_big_cycle() {
        try (RocksTransaction transaction = session.transaction(READ)) {
            final String queryString = "match " +
                    "  $a sub attribute, owns $b;" +
                    "  $b sub attribute, owns $c;" +
                    "  $c sub attribute, owns $d;" +
                    "  $d sub attribute, owns $a;";
            ResourceIterator<ConceptMap> answers = transaction.query().match(parseQuery(queryString).asMatch(), false);
            assertNotNulls(answers);
            assertTrue(answers.hasNext());
            Map<String, Set<String>> result = retrieveAnswers(answers);
            assertEquals(4, result.keySet().size());

            Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
                put("a", set("firstname", "surname", "nickname", "middlename"));
                put("b", set("firstname", "surname", "nickname", "middlename"));
                put("c", set("firstname", "surname", "nickname", "middlename"));
                put("d", set("firstname", "surname", "nickname", "middlename"));
            }};

            assertEquals(expected, result);
        }
    }

    @Test
    public void test_relates() {
        try (RocksTransaction transaction = session.transaction(READ)) {
            final String queryString = "match " +
                    "   $r relates wife;";
            ResourceIterator<ConceptMap> answers = transaction.query().match(parseQuery(queryString).asMatch(), false);
            assertNotNulls(answers);
            assertTrue(answers.hasNext());
            Map<String, Set<String>> result = retrieveAnswers(answers);
            assertEquals(1, result.keySet().size());

            Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
                put("r", set("marriage"));
            }};

            assertEquals(expected, result);
        }
    }

    @Test
    public void temp() {
        try (RocksTransaction transaction = session.transaction(READ)) {
            final String queryString = "match " +
                    "   $yoko plays relation:wife;";

            ResourceIterator<ConceptMap> answers = transaction.query().match(parseQuery(queryString).asMatch(), false);
            assertNotNulls(answers);
            assertTrue(answers.hasNext());
            Map<String, Set<String>> result = retrieveAnswers(answers);
//            assertEquals(3, result.keySet().size());

            Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
                put("yoko", set("woman"));
            }};

            assertEquals(expected, result);
        }
    }
}
