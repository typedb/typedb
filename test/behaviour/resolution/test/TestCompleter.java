package grakn.core.test.behaviour.resolution.test;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.behaviour.resolution.complete.Completer;
import grakn.core.test.behaviour.resolution.complete.SchemaManager;
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

public class TestCompleter {

    @ClassRule
    public static final GraknTestServer graknTestServer = new GraknTestServer();

    @Test
    public void testValidResolutionHasExactlyOneAnswer() {
        Set<Statement> expectedResolutionStatements = getStatements(Graql.parsePatternList("" +
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

        try (Session session = graknTestServer.sessionWithNewKeyspace()) {

            loadTestCase(session, "case4");

            Completer completer = new Completer(session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                completer.loadRules(tx, SchemaManager.getAllRules(tx));
            }

            SchemaManager.undefineAllRules(session);
            SchemaManager.addResolutionSchema(session);
            SchemaManager.connectResolutionSchema(session);
            completer.complete();

            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
                List<ConceptMap> answers = tx.execute(Graql.match(expectedResolutionStatements).get());

                assertEquals(answers.size(), 1);
            }
        }
    }

    @Test
    public void testDeduplicationOfInferredConcepts() {
        try (Session session = graknTestServer.sessionWithNewKeyspace()) {

            loadTestCase(session, "case1");

            Completer completer = new Completer(session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                completer.loadRules(tx, SchemaManager.getAllRules(tx));
            }

            SchemaManager.undefineAllRules(session);
            SchemaManager.addResolutionSchema(session);
            SchemaManager.connectResolutionSchema(session);
            completer.complete();

            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
                GraqlGet inferredAnswersQuery = Graql.match(Graql.var("lh").isa("location-hierarchy")).get();
                List<ConceptMap> inferredAnswers = tx.execute(inferredAnswersQuery);
                assertEquals(6, inferredAnswers.size());

                GraqlGet resolutionsQuery = Graql.match(Graql.var("res").isa("resolution")).get();
                List<ConceptMap> resolutionAnswers = tx.execute(resolutionsQuery);
                assertEquals(4, resolutionAnswers.size());
            }
        }
    }
}