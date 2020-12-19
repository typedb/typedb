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
import graql.lang.query.GraqlInsert;
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
                final String queryString = "define\n" +
                        "animal sub entity;\n" +
                        "" +
                        "mammal sub animal;\n" +
                        "reptile sub animal;\n" +
                        "tortoise sub reptile;\n" +
                        "" +
                        "person sub mammal,\n" +
                        "    owns name,\n" +
                        "    owns email,\n" +
                        "    plays marriage:spouse;\n" +
                        "" +
                        "man sub person,\n" +
                        "    plays marriage:husband;\n" +
                        "" +
                        "woman sub person,\n" +
                        "    plays marriage:wife;\n" +
                        "" +
                        "dog sub mammal,\n" +
                        "    owns name," +
                        "    owns tail-length;" +
                        "" +
                        "rat sub mammal,\n" +
                        "   owns tail-length;\n" +
                        "" +
                        "name sub attribute, value string;\n" +
                        "email sub attribute, value string;\n" +
                        "tail-length sub attribute, value long;\n" +
                        "" +
                        "marriage sub relation,\n" +
                        "    relates husband,\n" +
                        "    relates wife,\n" +
                        "    relates spouse;\n";
                final GraqlDefine query = parseQuery(queryString);
                transaction.query().define(query);
                transaction.commit();
            }
        }

        try (RocksSession session = grakn.session(database, DATA)) {
            try (RocksTransaction transaction = session.transaction(WRITE)) {
                final String queryString = "insert\n" +
                        "      $a isa woman, has name 'alice';\n" +
                        "      $b isa man, has name 'bob';\n" +
                        "      $c isa man, has name 'charlie';\n" +
                        "      (wife: $a, husband: $b) isa marriage;\n";
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
    public void test_owns_inheritance() {
        try (RocksTransaction transaction = session.transaction(READ)) {
            final String queryString = "match $p owns name;";
            ResourceIterator<ConceptMap> answers = transaction.query().match(parseQuery(queryString).asMatch(), false);
            assertNotNulls(answers);
            assertTrue(answers.hasNext());
            Map<String, Set<String>> result = retrieveAnswers(answers);
            assertEquals(1, result.keySet().size());

            Map<String, Set<String>> expected = new HashMap<String, Set<String>>(){{
                put("p", set("person", "man", "woman", "dog"));
            }};

            assertEquals(expected, result);
        }
    }

    @Test
    public void test_owns_cycle() {
        try (RocksTransaction transaction = session.transaction(READ)) {
            final String queryString = "match $a owns $b; $b owns $a;";
            ResourceIterator<ConceptMap> answers = transaction.query().match(parseQuery(queryString).asMatch(), false);
            assertNotNulls(answers);
            assertTrue(answers.hasNext());
            Map<String, Set<String>> result = retrieveAnswers(answers);
            assertEquals(1, result.keySet().size());

            Map<String, Set<String>> expected = new HashMap<>();

            assertEquals(expected, result);
        }
    }

    @Test
    public void test_relation_concrete_role() {
        try (RocksTransaction transaction = session.transaction(READ)) {
            final String queryString = "match " +
                    "   $r sub marriage, relates $role, relates wife;" +
                    "   $yoko plays $role, plays relation:wife;";
            ResourceIterator<ConceptMap> answers = transaction.query().match(parseQuery(queryString).asMatch(), false);
            assertNotNulls(answers);
            assertTrue(answers.hasNext());
            Map<String, Set<String>> result = retrieveAnswers(answers);
            assertEquals(1, result.keySet().size());

            Map<String, Set<String>> expected = new HashMap<String, Set<String>>(){{
                put("r", set("marriage"));
                put("yoko", set("woman"));
            }};

            assertEquals(expected, result);
        }
    }
}
