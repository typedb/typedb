package grakn.core.test.behaviour.resolution.test;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.behaviour.resolution.resolve.QueryBuilder;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static grakn.core.test.behaviour.resolution.common.Utils.getStatements;
import static grakn.core.test.behaviour.resolution.test.LoadTest.loadTestCase;
import static org.junit.Assert.assertEquals;

public class QueryBuilderIT {

    private static final String GRAKN_KEYSPACE = "query_builder_it";

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

        try (Session session = graknTestServer.session(GRAKN_KEYSPACE)) {

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

        try (Session session = graknTestServer.session(GRAKN_KEYSPACE)) {

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

    @Test
    public void testKeysStatementsAreGeneratedCorrectly() {
        GraqlGet inferenceQuery = Graql.parse("match $transaction isa transaction, has currency $currency; get;");

        Set<Statement> keyStatements;

        try (Session session = graknTestServer.session(GRAKN_KEYSPACE)) {

            loadTestCase(session, "case2");

            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
                ConceptMap answer = tx.execute(inferenceQuery).get(0);
                keyStatements = QueryBuilder.generateKeyStatements(answer.map());
            }
        }

        Set<Statement> expectedStatements = getStatements(Graql.parsePatternList(
                "$transaction has transaction-id 0;\n" +
                        "$currency \"GBP\";\n"
        ));

        assertEquals(expectedStatements, keyStatements);
    }
}