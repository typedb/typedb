package grakn.core.test.behaviour.resolution.test;

import grakn.client.GraknClient;
import grakn.client.answer.ConceptMap;
import grakn.verification.resolution.complete.Completer;
import grakn.verification.resolution.complete.SchemaManager;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static grakn.verification.resolution.common.Utils.getStatements;
import static grakn.verification.resolution.test.LoadTest.loadTestCase;
import static org.junit.Assert.assertEquals;

public class TestCompleter {

    private static final String GRAKN_URI = "localhost:48555";
    private static final String GRAKN_KEYSPACE = "query_completer";
    private static GraknForTest graknForTest;
    private static GraknClient graknClient;

    @BeforeClass
    public static void beforeClass() throws InterruptedException, IOException, TimeoutException {
        Path graknArchive = Paths.get("external", "graknlabs_grakn_core", "grakn-core-all-linux.tar.gz");
        graknForTest = new GraknForTest(graknArchive);
        graknForTest.start();
        graknClient = new GraknClient(GRAKN_URI);
    }

    @AfterClass
    public static void afterClass() throws InterruptedException, IOException, TimeoutException {
        graknForTest.stop();
    }

    @After
    public void after() {
        graknClient.keyspaces().delete(GRAKN_KEYSPACE);
    }

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

        try (GraknClient.Session session = graknClient.session(GRAKN_KEYSPACE)) {

            loadTestCase(session, "case4");

            Completer completer = new Completer(session);
            try (GraknClient.Transaction tx = session.transaction().write()) {
                completer.loadRules(tx, SchemaManager.getAllRules(tx));
            }

            SchemaManager.undefineAllRules(session);
            SchemaManager.addResolutionSchema(session);
            SchemaManager.connectResolutionSchema(session);
            completer.complete();

            try (GraknClient.Transaction tx = session.transaction().read()) {
                List<ConceptMap> answers = tx.execute(Graql.match(expectedResolutionStatements).get());

                assertEquals(answers.size(), 1);
            }
        }
    }

    @Test
    public void testDeduplicationOfInferredConcepts() {
        try (GraknClient.Session session = graknClient.session(GRAKN_KEYSPACE)) {

            loadTestCase(session, "case1");

            Completer completer = new Completer(session);
            try (GraknClient.Transaction tx = session.transaction().write()) {
                completer.loadRules(tx, SchemaManager.getAllRules(tx));
            }

            SchemaManager.undefineAllRules(session);
            SchemaManager.addResolutionSchema(session);
            SchemaManager.connectResolutionSchema(session);
            completer.complete();

            try (GraknClient.Transaction tx = session.transaction().read()) {
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