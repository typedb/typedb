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
import grakn.core.common.parameters.Arguments;
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
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.parameters.Arguments.Session.Type.DATA;
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

        try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction transaction = session.transaction(WRITE)) {
                final String queryString = "define\n" +
                        "      person sub entity,\n" +
                        "        plays friendship:friend,\n" +
                        "        plays employment:employee,\n" +
                        "        owns name,\n" +
                        "        owns age,\n" +
                        "        owns ref @key;\n" +
                        "      company sub entity,\n" +
                        "        plays employment:employer,\n" +
                        "        owns name,\n" +
                        "        owns ref @key;\n" +
                        "      friendship sub relation,\n" +
                        "        relates friend,\n" +
                        "        owns ref @key;\n" +
                        "      employment sub relation,\n" +
                        "        relates employee,\n" +
                        "        relates employer,\n" +
                        "        owns ref @key;\n" +
                        "      name sub attribute, value string;\n" +
                        "      age sub attribute, value long;\n" +
                        "      ref sub attribute, value long;";
                final GraqlDefine query = parseQuery(queryString);
                transaction.query().define(query);
                transaction.commit();
            }
        }

        try (RocksSession session = grakn.session(database, DATA)) {
            try (RocksTransaction transaction = session.transaction(WRITE)) {
                final String queryString = "insert $p isa person, has ref 0;\n" +
                        "$c isa company, has ref 1;\n" +
                        "$c2 isa company, has ref 2;\n" +
                        "$r (employee: $p, employer: $c, employer: $c2) isa employment, has ref 3;";
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
    public void all_combinations_of_players_in_a_relation_can_be_retrieved_0_repeat() {
        int success = 0, fail = 0;
        for (int i = 0; i < 10; i++) {
            try {
                all_combinations_of_players_in_a_relation_can_be_retrieved_0();
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
    public void all_combinations_of_players_in_a_relation_can_be_retrieved_0() {
        try (RocksTransaction transaction = session.transaction(READ)) {
            final String queryString = "match $r ($x, $y) isa employment;";
            ResourceIterator<ConceptMap> answers = transaction.query().match(parseQuery(queryString).asMatch());
            assertTrue(answers.hasNext());
            ConceptMap answer;
            AttributeType.Long ref = transaction.concepts().getAttributeType("ref").asLong();
            int count = 0;
            while (answers.hasNext()) {
                count++;
                answer = answers.next();
                System.out.println(answer.get("r").asThing().getHas(ref).findFirst().get().getValue());
                System.out.println(answer.get("x").asThing().getHas(ref).findFirst().get().getValue());
                System.out.println(answer.get("y").asThing().getHas(ref).findFirst().get().getValue());
            }
            assertEquals(6, count);
        }
    }

    @Test
    public void all_combinations_of_players_in_a_relation_can_be_retrieved_1() {
        Traversal.Parameters params = new Traversal.Parameters();
        GraphProcedure.Builder proc = GraphProcedure.builder(3);

        ProcedureVertex.Type relation = proc.labelled("employment", true);
        ProcedureVertex.Thing r = proc.nameThing("r");
        ProcedureVertex.Thing x = proc.nameThing("x");
        ProcedureVertex.Thing y = proc.nameThing("y");

        proc.setLabel(relation, "relation");

        proc.backwardIsa(1, relation, r, true);
        proc.forwardRolePlayer(2, r, x, set());
        proc.forwardRolePlayer(3, r, y, set());

        try (RocksTransaction transaction = session.transaction(READ)) {
            ResourceIterator<VertexMap> vertices = transaction.traversal().iterator(proc.build(), params);
            ResourceIterator<ConceptMap> answers = transaction.concepts().conceptMaps(vertices);
            AttributeType.Long ref = transaction.concepts().getAttributeType("ref").asLong();
            ConceptMap answer;
            int count = 0;
            while (answers.hasNext()) {
                count++;
                answer = answers.next();
                System.out.println(answer.get("r").asThing().getHas(ref).findFirst().get().getValue());
                System.out.println(answer.get("x").asThing().getHas(ref).findFirst().get().getValue());
                System.out.println(answer.get("y").asThing().getHas(ref).findFirst().get().getValue());
            }
            assertEquals(6, count);
        }
    }

    @Ignore
    @Test
    public void relations_are_matchable_from_roleplayers_without_specifying_any_roles_2() {
        Traversal.Parameters params = new Traversal.Parameters();
        GraphProcedure.Builder proc = GraphProcedure.builder(3);

        ProcedureVertex.Type person = proc.labelled("person", true);
        ProcedureVertex.Type relation = proc.labelled("relation");
        ProcedureVertex.Thing x = proc.nameThing("x");
        ProcedureVertex.Thing r = proc.nameThing("r");

        proc.setLabel(person, "person");
        proc.setLabel(relation, "relation");

        proc.backwardIsa(1, person, x, true);
        proc.backwardRolePlayer(2, x, r, set());
        proc.forwardIsa(3, r, relation, true);

        try (RocksTransaction transaction = session.transaction(READ)) {
            ResourceIterator<VertexMap> vertices = transaction.traversal().iterator(proc.build(), params);
            ResourceIterator<ConceptMap> answers = transaction.concepts().conceptMaps(vertices);
            assertTrue(answers.hasNext());
        }
    }
}
