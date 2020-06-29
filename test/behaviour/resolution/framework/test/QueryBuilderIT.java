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

package grakn.core.test.behaviour.resolution.framework.test;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.behaviour.resolution.framework.resolve.QueryBuilder;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static grakn.core.test.behaviour.resolution.framework.common.Utils.getStatements;
import static grakn.core.test.behaviour.resolution.framework.test.LoadTest.loadTestCase;
import static org.junit.Assert.assertEquals;

public class QueryBuilderIT {

    @ClassRule
    public static final GraknTestServer graknTestServer = new GraknTestServer();

    @Test
    public void testMatchGetQueryIsCorrect() {
        Set<Statement> expectedResolutionStatements = getStatements(Graql.parsePatternList(
                "$r0-com isa company;" +
                "$r0-com has is-liable $r0-lia;" +
                "$r0-com has company-id 0;" +
                "$r0-lia true;" +
                "$r1-c2 isa company, has name $r1-n2;" +
                "$r1-n2 \"the-company\";" +
                "$r1-l2 true;" +
                "$r1-c2 has is-liable $r1-l2;" +
                "$x0 (instance: $r1-c2) isa isa-property, has type-label \"company\";" +
                "$x1 (owner: $r1-c2) isa has-attribute-property, has name $r1-n2;" +
                "$x2 (owner: $r1-c2) isa has-attribute-property, has is-liable $r1-l2;" +
                "$_ (body: $x0, body: $x1, head: $x2) isa resolution, has rule-label \"company-is-liable\";" +
                "$r1-n2 == \"the-company\";" +
                "$r1-c2 has name $r1-n2;" +
                "$r1-c2 isa company;" +
                "$r1-c2 has company-id 0;" +
                "$r2-c1 isa company;" +
                "$r2-n1 \"the-company\";" +
                "$r2-c1 has name $r2-n1;" +
                "$x3 (instance: $r2-c1) isa isa-property, has type-label \"company\";" +
                "$x4 (owner: $r2-c1) isa has-attribute-property, has name $r2-n1;" +
                "$_ (body: $x3, head: $x4) isa resolution, has rule-label \"company-has-name\";" +
                "$r2-c1 has company-id 0;"
        ));

        GraqlGet inferenceQuery = Graql.parse("match $com isa company, has is-liable $lia; get;");

        try (Session session = graknTestServer.sessionWithNewKeyspace()) {

            loadTestCase(session, "case4");

            QueryBuilder qb = new QueryBuilder();
            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
                List<GraqlGet> kbCompleteQueries = qb.buildMatchGet(tx, inferenceQuery);
                GraqlGet kbCompleteQuery = kbCompleteQueries.get(0);
                Set<Statement> statements = kbCompleteQuery.match().getPatterns().statements();

                assertEquals(expectedResolutionStatements, statements);
            }
        }
    }

    @Test
    public void testMatchGetQueryIsCorrect_case5() {

        Set<Statement> expectedResolutionStatements = getStatements(Graql.parsePatternList("" +
                "$r0-c isa company;" +
                "$r0-c has name $r0-n;" +
                "$r0-n \"the-company\";" +
                "$r0-c has company-id 0;" +
                "$r1-c isa company;" +
                "$r1-c has name $r1-n;" +
                "$r1-n \"the-company\";" +
                "$x0 (instance: $r1-c) isa isa-property, has type-label \"company\";" +
                "$x1 (owner: $r1-c) isa has-attribute-property, has name $r1-n;" +
                "$_ (body: $x0, head: $x1) isa resolution, has rule-label \"company-has-name\";" +
                "$r1-c has company-id 0;"
        ));

        GraqlGet inferenceQuery = Graql.parse("match $c isa company, has name $n; get;");

        try (Session session = graknTestServer.sessionWithNewKeyspace()) {

            loadTestCase(session, "case5");

            QueryBuilder qb = new QueryBuilder();
            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
                List<GraqlGet> kbCompleteQueries = qb.buildMatchGet(tx, inferenceQuery);
                GraqlGet kbCompleteQuery = kbCompleteQueries.get(0);
                Set<Statement> statements = kbCompleteQuery.match().getPatterns().statements();

                assertEquals(expectedResolutionStatements, statements);
            }
        }
    }

//    @Test
//    public void testMatchGetQueryIsCorrect_case6() {
//
//
//        Set<Statement> expectedResolutionStatements = getStatements(Graql.parsePatternList("" +
////                // Level 0
////                "{ $c id V8352; { $c isa company; $c has name $n1; $n1 == \"the-company\"; not { $c has is-liable $lia; }; } or { $n2 == \"another-company\"; $c isa company; $c has name $n2; not { $c has is-liable $lia; }; }; };" +
////                // Level 1
////                "{ $n2 == \"another-company\"; $c id V8352; $c isa company; $n2 id V12464; $c has name $n2; not { $c has is-liable $lia; }; };" +
////                //Level 2
////                "{ $n2 == \"another-company\"; $c isa company; $c id V8352; $n2 id V12464; $c has name $n2; };" +
//
//
//                // Level 0
//                "{ $r0-c has company-id 1; { $r0-c isa company; $r0-c has name $r0-n1; $r0-n1 == \"the-company\"; not { $r0-c has is-liable $r0-lia; }; } or { $r0-n2 == \"another-company\"; $r0-c isa company; $r0-c has name $r0-n2; not { $r0-c has is-liable $r0-lia; }; }; };" +
//                // Level 1
//                "{ $r1-n2 == \"another-company\"; $r1-c id V8352; $r1-c isa company; $r1-n2 id V12464; $r1-c has name $r1-n2; not { $r1-c has is-liable $r1-lia; }; };" +
//                //Level 2
//                "{ $r2-n2 == \"another-company\"; $r2-c isa company; $r2-c id V8352; $r2-n2 id V12464; $r2-c has name $r2-n2; };" +
//
//                ""));
//
//        Set<Statement> expectedResolutionStatements = getStatements(Graql.parsePatternList("" +
//                "$r0-c isa company; " +
//                "{$r0-c has name $r0-n1; $r0-n1 \"the-company\";} or {$r0-c has name $r0-n2; $r0-n2 \"another-company\";}; " +
//                "not {$r0-c has is-liable $r0-lia;}; " +
//                "$r0-c has company-id 0;" +
//                "$r1-c isa company;" +
//                "$r1-c has name $r1-n;" +
//                "$r1-n2 \"another-company\";" +
//                "$x0 (instance: $r1-c) isa isa-property, has type-label \"company\";" +
//                "$x1 (owner: $r1-c) isa has-attribute-property, has name $r1-n2;" +
//                "$_ (body: $x0, head: $x1) isa resolution, has rule-label \"company-has-name\";" +
//                "$r1-c has company-id 0;"
//        ));
//
//        GraqlGet inferenceQuery = Graql.parse("" +
//                "match $c isa company; " +
//                "{$c has name $n1; $n1 \"the-company\";} or {$c has name $n2; $n2 \"another-company\";}; " +
//                "not {$c has is-liable $lia;}; " +
//                "get;").asGet();
//
//        try (Session session = graknTestServer.sessionWithNewKeyspace()) {
//
//            loadTestCase(session, "case6");
//
//            QueryBuilder qb = new QueryBuilder();
//            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
//                List<GraqlGet> kbCompleteQueries = qb.buildMatchGet(tx, inferenceQuery);
//                GraqlGet kbCompleteQuery = kbCompleteQueries.get(0);
//                Set<Statement> statements = kbCompleteQuery.match().getPatterns().statements();
//
//                assertEquals(expectedResolutionStatements, statements);
//            }
//        }
//    }

    @Test
    public void testMatchGetQueryIsCorrect_case8() {

        Pattern expectedResolutionPattern = Graql.parsePattern("" +
//                // Level 0
                "{ $r0-c has company-id 0; { $r0-n2 == \"some-name\"; $r0-c isa company; $r0-c has name $r0-n2; } or { $r0-c isa company; $r0-n1 == \"the-company\"; $r0-c has name $r0-n1; }; " +
//                // Level 1
                "$r1-c isa company; $r1-n1 == \"the-company\"; $r1-n1 \"the-company\"; $r1-c has company-id 0; $r1-c has name $r1-n1; " +
                "$x0 (instance: $r1-c) isa isa-property, has type-label \"company\"; " +
                "$x1 (owner: $r1-c) isa has-attribute-property, has name $r1-n; " +
                "$_ (body: $x0, head: $x1) isa resolution, has rule-label \"company-has-name\"; " +
//                //Level 2
                "$r2-c2 has company-id 0; $r2-c2 isa company; };");

        /**
         * Actual:
         * {
         *  {
         *      {
         *          { $r0-n2 == "some-name"; $r0-c isa company; $r0-c has name $r0-n2; } or { $r0-c has name $r0-n1; $r0-c isa company; $r0-n1 == "the-company"; };
         *      };
         *      $r0-c has company-id 0;
         *      { $r0-c has name $r0-n1; $r0-c isa company; $r0-n1 == "the-company"; };
         *      $r0-n1 "the-company" isa name;
         *      { $r1-c2 isa company; };
         *      { $r1-n2 "the-company"; $r1-c2 has name $r1-n2; };
         *      {
         *          {
         *              { $x0 (body: $x0); $x0 (instance: $r1-c2) isa isa-property, has type-label "company"; };
         *          };
         *          {
         *              {  };
         *              { $x0 (head: $x1); $x1 (owner: $r1-c2) isa has-attribute-property, has name $r1-n2; };
         *          };
         *          $x0 isa resolution, has rule-label "company-has-name";
         *      };
         *      {
         *          { $r1-c2 isa company; };
         *          $r1-c2 has company-id 0;
         *      };
         *   };
         * };
         *
         * {
         *  {
         *      {
         *          { $r0-c isa company; $r0-c has name $r0-n2; $r0-n2 == "some-name"; } or { $r0-c has name $r0-n1; $r0-c isa company; $r0-n1 == "the-company"; };
         *      };
         *      $r0-c has company-id 0;
         *      { $r0-c has name $r0-n1; $r0-c isa company; $r0-n1 == "the-company"; };
         *      $r0-n1 "the-company" isa name;
         *      { $r1-c2 isa company; };
         *      { $r1-c2 has name $r1-n2; $r1-n2 "the-company"; };
         *      {
         *          { $x0 (instance: $r1-c2) isa isa-property, has type-label "company"; $x0 (body: $x0); };
         *          { $x0 (head: $x1); $x1 (owner: $r1-c2) isa has-attribute-property, has name $r1-n2; };
         *          $x0 isa resolution, has rule-label "company-has-name";
         *      };
         *      {
         *          { $r1-c2 isa company; };
         *          $r1-c2 has company-id 0;
         *      };
         *  };
         * };
         *
         * {
         *  {
         *  $r0-n1 "the-company" isa name;
         *  { $r1-c2 has company-id 0; $r1-c2 isa company; };
         *  { $r0-c has name $r0-n1; $r0-c isa company; $r0-n1 == "the-company"; };
         *  { $x0 isa resolution, has rule-label "company-has-name";
         *      { $x0 (head: $x1); $x1 (owner: $r1-c2) isa has-attribute-property, has name $r1-n2; };
         *      { $x0 (instance: $r1-c2) isa isa-property, has type-label "company"; $x0 (body: $x0); };
         *  };
         *  $r1-c2 isa company;
         *  { $r0-c has name $r0-n1; $r0-c isa company; $r0-n1 == "the-company"; } or { $r0-c isa company; $r0-c has name $r0-n2; $r0-n2 == "some-name"; };
         *  { $r1-n2 "the-company"; $r1-c2 has name $r1-n2; };
         *  $r0-c has company-id 0;
         *  };
         *  };
         *
         *  {
         *      {
         *          $r0-c has company-id 0; $r0-c has name $r0-n1; $r1-n2 "the-company"; $x0 (head: $x1); $r0-c isa company; $r0-n1 "the-company" isa name; $r1-c2 has company-id 0; $x1 (owner: $r1-c2) isa has-attribute-property, has name $r1-n2; $r0-n1 == "the-company";
         *          { $r0-c has name $r0-n1; $r0-c isa company; $r0-n1 == "the-company"; } or { $r0-c isa company; $r0-c has name $r0-n2; $r0-n2 == "some-name"; };
         *          $x0 (body: $x0); $x0 isa resolution, has rule-label "company-has-name"; $x0 (instance: $r1-c2) isa isa-property, has type-label "company"; $r1-c2 isa company; $r1-c2 has name $r1-n2;
         *      };
         *  };
         *
         *  {
         *      {
         *          $r0-c has name $r0-n1; $x0 (instance: $r1-c2) isa isa-property, has type-label "company"; $x0 (head: $x1); $r0-c isa company; $r0-n1 "the-company" isa name; $x0 isa resolution, has rule-label "company-has-name"; $x1 (owner: $r1-c2) isa has-attribute-property, has name $r1-n2; $r1-n2 "the-company"; $r1-c2 has company-id 0; $r0-c has company-id 0; $x0 (body: $x0);
         *          { $r0-c isa company; $r0-c has name $r0-n2; $r0-n2 == "some-name"; } or { $r0-c has name $r0-n1; $r0-c isa company; $r0-n1 == "the-company"; };
         *          $r0-n1 == "the-company"; $r1-c2 isa company; $r1-c2 has name $r1-n2;
         *      };
         *  };
         *
         *  {
         *      $r0-c has name $r0-n1; $r0-n1 "the-company" isa name; $x0 (head: $x1); $r0-c isa company; $r1-n2 "the-company";
         *      { $r0-c has name $r0-n1; $r0-c isa company; $r0-n1 == "the-company"; } or { $r0-c isa company; $r0-c has name $r0-n2; $r0-n2 == "some-name"; };
         *      $x0 isa resolution, has rule-label "company-has-name"; $r1-c2 has company-id 0; $x1 (owner: $r1-c2) isa has-attribute-property, has name $r1-n2; $r0-n1 == "the-company"; $x0 (instance: $r1-c2) isa isa-property, has type-label "company"; $x0 (body: $x0); $r0-c has company-id 0; $r1-c2 isa company; $r1-c2 has name $r1-n2;
         *  };
         *
         *  {
         *      {
         *          $r0-c has name $r0-n1; $x0 (head: $x1); $r0-c isa company; $x0 (instance: $r1-c2) isa isa-property, has type-label "company"; $x1 (owner: $r1-c2) isa has-attribute-property, has name $r1-n2; $r0-n1 "the-company" isa name; $x0 isa resolution, has rule-label "company-has-name"; $r0-c has company-id 0; $r1-n2 "the-company"; $x0 (body: $x0); $r1-c2 has company-id 0; $r0-n1 == "the-company"; $r1-c2 isa company; $r1-c2 has name $r1-n2;
         *          { $r0-c isa company; $r0-c has name $r0-n2; $r0-n2 == "some-name"; } or { $r0-c has name $r0-n1; $r0-c isa company; $r0-n1 == "the-company"; };
         *      };
         *  };
         *
         *  {
         *      {
         *          $r0-c has name $r0-n1; $r0-c isa company; $r1-n2 "the-company"; $r0-n1 "the-company" isa name; $r1-c2 has company-id 0; $x0 (instance: $r1-c2) isa isa-property, has type-label "company"; $rule0 isa resolution, has rule-label "company-has-name"; $x1 (owner: $r1-c2) isa has-attribute-property, has name $r1-n2; $r0-n1 == "the-company"; $rule0 (body: $x0); $rule0 (head: $x1);
         *          { $r0-c isa company; $r0-c has name $r0-n2; $r0-n2 == "some-name"; } or { $r0-c has name $r0-n1; $r0-c isa company; $r0-n1 == "the-company"; };
         *          $r1-c2 isa company; $r1-c2 has name $r1-n2; $r0-c has company-id 0;
         *      };
         *  };
         *
         *
         */

        GraqlGet inferenceQuery = Graql.parse("" +
                "match $c isa company; " +
                "{$c has name $n1; $n1 \"the-company\";} or {$c has name $n2; $n2 \"some-name\";}; " +
                "get;").asGet();

        try (Session session = graknTestServer.sessionWithNewKeyspace()) {

            loadTestCase(session, "case8");

            QueryBuilder qb = new QueryBuilder();
            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
                List<GraqlGet> kbCompleteQueries = qb.buildMatchGet(tx, inferenceQuery);
                GraqlGet kbCompleteQuery = kbCompleteQueries.get(0);
                Pattern pattern = kbCompleteQuery.match().getPatterns();

                assertEquals(expectedResolutionPattern, pattern);
            }
        }
    }
/**
 * Expected :{ $r0-c has company-id 0; { $r0-n2 == "some-name"; $r0-c isa company; $r0-c has name $r0-n2; } or { $r0-c isa company; $r0-n1 == "the-company"; $r0-c has name $r0-n1; }; $r1-c isa company; $r1-n1 == "the-company"; $r1-n1 "the-company"; $r1-c has company-id 0; $r1-c has name $r1-n1; $x0 (instance: $r1-c) isa isa-property, has type-label "company"; $x1 (owner: $r1-c) isa has-attribute-property, has name $r1-n; $_ (body: $x0, head: $x1) isa resolution, has rule-label "company-has-name"; $r2-c2 has company-id 0; $r2-c2 isa company; };
 * Actual   :{ $r0-c has name $r0-n1; $r0-c isa company; $r0-c has name $r0-n2; $r0-n1 == "the-company"; $r0-n2 == "some-name"; $r0-c has company-id 0; $r0-n1 "the-company"; $r1-c2 isa company; $r1-n2 "the-company"; $r1-c2 has name $r1-n2; $x0 (instance: $r1-c2) isa isa-property, has type-label "company"; $x1 (owner: $r1-c2) isa has-attribute-property, has name $r1-n2; $_ (body: $x0, head: $x1) isa resolution, has rule-label "company-has-name"; $r1-c2 has company-id 0; };
 */




    @Test
    public void testKeysStatementsAreGeneratedCorrectly() {
        GraqlGet inferenceQuery = Graql.parse("match $transaction isa transaction, has currency $currency; get;");

        Set<Statement> keyStatements;

        try (Session session = graknTestServer.sessionWithNewKeyspace()) {

            loadTestCase(session, "case2");

            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
                ConceptMap answer = tx.execute(inferenceQuery).get(0);
                keyStatements = QueryBuilder.generateKeyStatements(answer.map());
            }
        }

        Set<Statement> expectedStatements = getStatements(Graql.parsePatternList(
                "$transaction has transaction-id 0;\n" +
                "$currency \"GBP\" isa currency;\n"
        ));

        assertEquals(expectedStatements, keyStatements);
    }
}