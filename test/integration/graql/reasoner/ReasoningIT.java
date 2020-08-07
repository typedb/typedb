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

package grakn.core.graql.reasoner;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Variable;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.test.common.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static grakn.core.test.common.GraqlTestUtil.loadFromFileAndCommit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Suite of tests checking different meanders and aspects of reasoning - full reasoning cycle is being tested.
 */
@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class ReasoningIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static String resourcePath = "test/integration/graql/reasoner/stubs/";

    //The tests validate the correctness of the rule reasoning implementation w.r.t. the intended semantics of rules.

    //tests if partial substitutions are propagated correctly - atom disjointness may lead to variable loss (bug #15476)
    @Test //Expected result: 2 relations obtained by correctly finding relations
    public void reasoningWithRelations() {
        try (Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet26.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.READ)) {

                String queryString = "match (role1: $x1, role2: $x2) isa relation2; get;";
                List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
                assertEquals(2, answers.size());

                String queryString2 = "match " +
                        "$b isa entity2;" +
                        "$b has res1 'value';" +
                        "$rel1 has res2 'value1';" +
                        "$rel1 (role1: $p, role2: $b) isa relation1;" +
                        "$rel2 has res2 'value2';" +
                        "$rel2 (role1: $c, role2: $b) isa relation1; get;";
                List<ConceptMap> answers2 = tx.execute(Graql.parse(queryString2).asGet());
                assertEquals(2, answers2.size());
                Set<Variable> vars = Sets.newHashSet(new Variable("b"),
                        new Variable("p"),
                        new Variable("c"),
                        new Variable("rel1"),
                        new Variable("rel2"));
                answers2.forEach(ans -> assertTrue(ans.vars().containsAll(vars)));
            }
        }
    }

    // TODO: maybe migrate this to a reasoner cache test suite?
    @Test
    public void whenAppendingRolePlayers_executingQueriesDoesNotModifyResults() {
        try (Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "appendingRPs.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
                List<ConceptMap> persistedRelations = tx.execute(Graql.parse("match $r isa baseRelation; get;").asGet(), false);

                tx.execute(Graql.<GraqlGet>parse("match (someRole: $x, anotherRole: $y, anotherRole: $z, inferredRole: $z); $y != $z;get;"));

                tx.execute(Graql.<GraqlGet>parse("match (someRole: $x, yetAnotherRole: $y, andYetAnotherRole: $y, inferredRole: $z); get;"));

                tx.execute(Graql.<GraqlGet>parse("match " +
                        "(someRole: $x, inferredRole: $z); " +
                        "not {(anotherRole: $z);};" +
                        "get;"));

                tx.execute(Graql.<GraqlGet>parse("match " +
                        "$r (someRole: $x, inferredRole: $z); " +
                        "not {$r (anotherRole: $z);};" +
                        "get;"));

                tx.execute(Graql.<GraqlGet>parse("match " +
                        "$r (someRole: $x, inferredRole: $z); " +
                        "not {$r (yetAnotherRole: $y, andYetAnotherRole: $y);};" +
                        "get;"));

                assertEquals("New relations were created!", persistedRelations, tx.execute(Graql.parse("match $r isa baseRelation; get;").asGet(), false));
            }
        }
    }

    @Test //when rule are defined to append new RPs no new relation instances should be created
    public void whenAppendingRolePlayers_noNewRelationsAreCreated() {
        try (Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "appendingRPs.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
                List<ConceptMap> persistedRelations = tx.execute(Graql.parse("match $r isa baseRelation; get;").asGet(), false);
                List<ConceptMap> inferredRelations = tx.execute(Graql.parse("match $r isa baseRelation; get;").asGet());
                assertCollectionsNonTriviallyEqual("New relations were created!", persistedRelations, inferredRelations);

                Set<ConceptMap> variants = Stream.of(
                        Iterables.getOnlyElement(tx.execute(Graql.<GraqlGet>parse("match $r (someRole: $x, anotherRole: $y, anotherRole: $z, inferredRole: $z); $y != $z;get;"), false)),
                        Iterables.getOnlyElement(tx.execute(Graql.<GraqlGet>parse("match $r (someRole: $x, inferredRole: $z ); $x has resource 'value'; get;"), false)),
                        Iterables.getOnlyElement(tx.execute(Graql.<GraqlGet>parse("match $r (someRole: $x, yetAnotherRole: $y, andYetAnotherRole: $y, inferredRole: $z); get;"), false)),
                        Iterables.getOnlyElement(tx.execute(Graql.<GraqlGet>parse("match $r (anotherRole: $x, andYetAnotherRole: $y); get;"), false))
                )
                        .map(ans -> ans.project(Sets.newHashSet(new Variable("r"))))
                        .collect(Collectors.toSet());

                assertCollectionsNonTriviallyEqual("Rules are not matched correctly!", variants, inferredRelations);
            }
        }
    }
}
