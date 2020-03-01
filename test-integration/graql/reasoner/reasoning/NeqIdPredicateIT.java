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
 */

package grakn.core.graql.reasoner.reasoning;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static grakn.core.util.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class NeqIdPredicateIT {

    private static String resourcePath = "test-integration/graql/reasoner/stubs/";

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Test //Expected result: 2 relations obtained by correctly finding reified relations
    public void reasoningWithNeqProperty() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet27.gql", session);
            try (Transaction tx = session.writeTransaction()) {
                                String queryString = "match (related-state: $s) isa holds; get;";

                List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
                List<ConceptMap> exact = tx.execute(Graql.parse("match $s isa state, has name 's2'; get;").asGet());
                assertCollectionsNonTriviallyEqual(exact, answers);
            }
        }
    }

    @Test //tests a query containing a neq predicate bound to a recursive relation
    public void recursiveRelationWithNeqPredicate(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet29.gql", session);
            try (Transaction tx = session.writeTransaction()) {
                                String baseQueryString = "match " +
                        "(role1: $x, role2: $y) isa binary-base;" +
                        "$x != $y;";
                String queryString = baseQueryString + "$y has name 'c'; get;";

                List<ConceptMap> baseAnswers = tx.execute(Graql.parse(baseQueryString + "get;").asGet());
                assertEquals(6, baseAnswers.size());
                baseAnswers.forEach(ans -> {
                    assertEquals(2, ans.size());
                    assertNotEquals(ans.get("x"), ans.get("y"));
                });

                String explicitString = "match " +
                        "(role1: $x, role2: $y) isa binary-base;" +
                        "$y has name 'c';" +
                        "{$x has name 'a';} or {$x has name 'b';}; get;";

                List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
                List<ConceptMap> answers2 = tx.execute(Graql.parse(explicitString).asGet());
                assertCollectionsNonTriviallyEqual(answers, answers2);
            }
        }
    }

    /**
     * Tests a scenario in which the neq predicate binds free variables of two recursive equivalent relations.
     * Corresponds to the following pattern:
     *
     *                     x
     *                   /    \
     *                 /        \
     *               v           v
     *              y     !=      z
     */
    @Test
    public void recursiveRelationsWithSharedNeqPredicate_relationsAreEquivalent(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet29.gql", session);
            try (Transaction tx = session.writeTransaction()) {
                                String baseQueryString = "match " +
                        "(role1: $x, role2: $y) isa binary-base;" +
                        "(role1: $x, role2: $z) isa binary-base;" +
                        "$y != $z;";

                List<ConceptMap> baseAnswers = tx.execute(Graql.parse(baseQueryString + "get;").asGet());
                assertEquals(18, baseAnswers.size());
                baseAnswers.forEach(ans -> {
                    assertEquals(3, ans.size());
                    assertNotEquals(ans.get("y"), ans.get("z"));
                });

                String queryString = baseQueryString + "$x has name 'a';";
                String explicitString = "match " +
                        "$x has name 'a';" +
                        "{$y has name 'a';$z has name 'b';} or " +
                        "{$y has name 'a';$z has name 'c';} or " +
                        "{$y has name 'b';$z has name 'a';} or" +
                        "{$y has name 'b';$z has name 'c';} or " +
                        "{$y has name 'c';$z has name 'a';} or " +
                        "{$y has name 'c';$z has name 'b';};";

                List<ConceptMap> answers = tx.execute(Graql.parse(queryString + "get;").asGet());
                List<ConceptMap> answers2 = tx.execute(Graql.parse(explicitString + "get;").asGet(), false);
                assertTrue(baseAnswers.containsAll(answers));
                assertCollectionsNonTriviallyEqual(answers, answers2);
            }
        }
    }

    /**
     * Tests a scenario in which the neq predicate prevents loops by binding free variables
     * of two recursive non-equivalent relations. Corresponds to the following pattern:
     *
     *                     y
     *                    ^  \
     *                  /      \
     *                /          v
     *              x     !=      z
     */
    @Test
    public void multipleRecursiveRelationsWithSharedNeqPredicate_neqPredicatePreventsLoops(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet29.gql", session);
            try (Transaction tx = session.writeTransaction()) {
                                String baseQueryString = "match " +
                        "(role1: $x, role2: $y) isa binary-base;" +
                        "(role1: $y, role2: $z) isa binary-base;" +
                        "$x != $z;";

                List<ConceptMap> baseAnswers = tx.execute(Graql.parse(baseQueryString + "get;").asGet());
                assertEquals(18, baseAnswers.size());
                baseAnswers.forEach(ans -> {
                    assertEquals(3, ans.size());
                    assertNotEquals(ans.get("x"), ans.get("z"));
                });

                String queryString = baseQueryString + "$x has name 'a';";

                String explicitString = "match " +
                        "$x has name 'a';" +
                        "{$y has name 'a';$z has name 'b';} or " +
                        "{$y has name 'a';$z has name 'c';} or " +
                        "{$y has name 'b';$z has name 'c';} or " +
                        "{$y has name 'b';$z has name 'b';} or " +
                        "{$y has name 'c';$z has name 'c';} or " +
                        "{$y has name 'c';$z has name 'b';};";

                List<ConceptMap> answers = tx.execute(Graql.parse(queryString + "get;").asGet());
                List<ConceptMap> answers2 = tx.execute(Graql.parse(explicitString + "get;").asGet(), false);
                assertCollectionsNonTriviallyEqual(answers, answers2);
            }
        }
    }

    /**
     * Tests a scenario in which the multiple neq predicates are present but bind at most single var in a relation.
     * Corresponds to the following pattern:
     *
     *              y       !=      z1
     *               ^              ^
     *                 \           /
     *                   \       /
     *                      x[a]
     *                   /      \
     *                 /          \
     *                v            v
     *              y2     !=      z2
     */
    @Test
    public void multipleRecursiveRelationsWithMultipleSharedNeqPredicates_symmetricPattern(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet29.gql", session);
            try (Transaction tx = session.writeTransaction()) {
                                String baseQueryString = "match " +
                        "(role1: $x, role2: $y1) isa binary-base;" +
                        "(role1: $x, role2: $z1) isa binary-base;" +
                        "(role1: $x, role2: $y2) isa binary-base;" +
                        "(role1: $x, role2: $z2) isa binary-base;" +

                        "$y1 != $z1;" +
                        "$y2 != $z2;";

                List<ConceptMap> baseAnswers = tx.execute(Graql.parse(baseQueryString + "get;").asGet());
                assertEquals(108, baseAnswers.size());
                baseAnswers.forEach(ans -> {
                    assertEquals(5, ans.size());
                    assertNotEquals(ans.get("y1"), ans.get("z1"));
                    assertNotEquals(ans.get("y2"), ans.get("z2"));
                });

                String queryString = baseQueryString + "$x has name 'a';";

                List<ConceptMap> answers = tx.execute(Graql.parse(queryString + "get;").asGet());
                assertEquals(36, answers.size());
                answers.forEach(ans -> {
                    assertEquals(5, ans.size());
                    assertNotEquals(ans.get("y1"), ans.get("z1"));
                    assertNotEquals(ans.get("y2"), ans.get("z2"));
                });
            }
        }
    }

    /**
     * Tests a scenario in which a single relation has both variables bound with two different neq predicates.
     * Corresponds to the following pattern:
     *
     *                  x[a]  - != - >  z1
     *                  |
     *                  |
     *                  v
     *                  y     - != - >  z2
     */
    @Test
    public void multipleRecursiveRelationsWithMultipleSharedNeqPredicates() {
        try (Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet29.gql", session);
            try (Transaction tx = session.writeTransaction()) {
                                String baseQueryString = "match " +
                        "(role1: $x, role2: $y) isa binary-base;" +
                        "$x != $z1;" +
                        "$y != $z2;" +
                        "(role1: $x, role2: $z1) isa binary-base;" +
                        "(role1: $y, role2: $z2) isa binary-base;";

                List<ConceptMap> baseAnswers = tx.execute(Graql.parse(baseQueryString + "get;").asGet());
                assertEquals(36, baseAnswers.size());
                baseAnswers.forEach(ans -> {
                    assertEquals(4, ans.size());
                    assertNotEquals(ans.get("x"), ans.get("z1"));
                    assertNotEquals(ans.get("y"), ans.get("z2"));
                });

                String queryString = baseQueryString + "$x has name 'a';";

                List<ConceptMap> answers = tx.execute(Graql.parse(queryString + "get;").asGet());
                assertEquals(12, answers.size());
                answers.forEach(ans -> {
                    assertEquals(4, ans.size());
                    assertNotEquals(ans.get("x"), ans.get("z1"));
                    assertNotEquals(ans.get("y"), ans.get("z2"));
                });
            }
        }
    }
}
