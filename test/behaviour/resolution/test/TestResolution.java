package grakn.core.test.behaviour.resolution.test;

import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.behaviour.resolution.Resolution;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static grakn.core.test.behaviour.resolution.common.Utils.loadGqlFile;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class TestResolution {

    private void initialiseKeyspace(Session session, Path schemaPath, Path dataPath) {
        try {
            // Load a schema incl. rules
            loadGqlFile(session, schemaPath);
            // Load data
            loadGqlFile(session, dataPath);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @ClassRule
    public static final GraknTestServer graknTestServer = new GraknTestServer();

    @Test
    public void testResolutionTestPassesWhenCorrect() {
        Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case1", "schema.gql").toAbsolutePath();
        Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case1", "data.gql").toAbsolutePath();
        GraqlGet inferenceQuery = Graql.parse("" +
                "match $lh (location-hierarchy_superior: $continent, " +
                "location-hierarchy_subordinate: $area) isa location-hierarchy; " +
                "$continent isa continent; " +
                "$area isa area; get;").asGet();


        Session completionSession = graknTestServer.sessionWithNewKeyspace();
        initialiseKeyspace(completionSession, schemaPath, dataPath);

        Session testSession = graknTestServer.sessionWithNewKeyspace();
        initialiseKeyspace(testSession, schemaPath, dataPath);

        Resolution resolution_test = new Resolution(completionSession, testSession);
        resolution_test.testQuery(inferenceQuery);
        resolution_test.testResolution(inferenceQuery);
        resolution_test.testCompleteness();
        resolution_test.close();
    }

    @Test
    public void testResolutionTestThrowsWhenRuleIsNotTriggered() {
        Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case1", "schema.gql").toAbsolutePath();
        Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case1", "data.gql").toAbsolutePath();
        GraqlGet inferenceQuery = Graql.parse("" +
                "match $lh (location-hierarchy_superior: $continent, " +
                "location-hierarchy_subordinate: $area) isa location-hierarchy; " +
                "$continent isa continent; " +
                "$area isa area; get;").asGet();


        Session completionSession = graknTestServer.sessionWithNewKeyspace();
        initialiseKeyspace(completionSession, schemaPath, dataPath);

        Session testSession = graknTestServer.sessionWithNewKeyspace();
        initialiseKeyspace(testSession, schemaPath, dataPath);

        // Undefine a rule in the keyspace under test such that the expected facts will not be inferred
        Transaction tx = testSession.transaction(Transaction.Type.WRITE);
        tx.execute(Graql.undefine(Graql.type("location-hierarchy-transitivity").sub("rule")));
        tx.commit();

        Resolution resolution_test = new Resolution(completionSession, testSession);

        Exception testQueryThrows = null;
        try {
            resolution_test.testQuery(inferenceQuery);
        } catch (Exception e) {
            testQueryThrows = e;
        }
        assertNotNull(testQueryThrows);
        assertThat(testQueryThrows, instanceOf(RuntimeException.class));

        Exception testResolutionThrows = null;
        try {
            resolution_test.testResolution(inferenceQuery);
        } catch (Exception e) {
            testResolutionThrows = e;
        }
        assertNotNull(testResolutionThrows);
        assertThat(testResolutionThrows, instanceOf(RuntimeException.class));

        Exception testCompletenessThrows = null;
        try {
            resolution_test.testCompleteness();
        } catch (Exception e) {
            testCompletenessThrows = e;
        }
        assertNotNull(testCompletenessThrows);
        assertThat(testCompletenessThrows, instanceOf(RuntimeException.class));

        resolution_test.close();
    }


    @Test
    public void testCase1HappyPath() {
        Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case1", "schema.gql").toAbsolutePath();
        Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case1", "data.gql").toAbsolutePath();
        GraqlGet inferenceQuery = Graql.parse("" +
                "match $lh (location-hierarchy_superior: $continent, " +
                "location-hierarchy_subordinate: $area) isa location-hierarchy; " +
                "$continent isa continent; " +
                "$area isa area; get;").asGet();

        Session completeSession = graknTestServer.sessionWithNewKeyspace();
        Session testSession = graknTestServer.sessionWithNewKeyspace();
        Resolution resolution_test = new Resolution(completeSession, testSession);
        resolution_test.testQuery(inferenceQuery);
        resolution_test.testCompleteness();
        resolution_test.close();
    }

    @Test
    public void testCase2HappyPath() {
        Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case2", "schema.gql").toAbsolutePath();
        Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case2", "data.gql").toAbsolutePath();
        GraqlGet inferenceQuery = Graql.parse("match $transaction has currency $currency; get;").asGet();

        Session completeSession = graknTestServer.sessionWithNewKeyspace();
        Session testSession = graknTestServer.sessionWithNewKeyspace();
        Resolution resolution_test = new Resolution(completeSession, testSession);
        resolution_test.testQuery(inferenceQuery);
        resolution_test.testCompleteness();
        resolution_test.close();
    }

    @Test
    public void testCase3HappyPath() {
        Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case3", "schema.gql").toAbsolutePath();
        Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case3", "data.gql").toAbsolutePath();
        GraqlGet inferenceQuery = Graql.parse("match (sibling: $p, sibling: $p1) isa siblingship; $p != $p1; get;").asGet();

        Session completeSession = graknTestServer.sessionWithNewKeyspace();
        Session testSession = graknTestServer.sessionWithNewKeyspace();
        Resolution resolution_test = new Resolution(completeSession, testSession);
        resolution_test.testQuery(inferenceQuery);
        resolution_test.testCompleteness();
        resolution_test.close();
    }

    @Test
    public void testCase4HappyPath() {
        Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case4", "schema.gql").toAbsolutePath();
        Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case4", "data.gql").toAbsolutePath();
        GraqlGet inferenceQuery = Graql.parse("match $com isa company, has is-liable $lia; get;").asGet();

        Session completeSession = graknTestServer.sessionWithNewKeyspace();
        Session testSession = graknTestServer.sessionWithNewKeyspace();
        Resolution resolution_test = new Resolution(completeSession, testSession);
        resolution_test.testQuery(inferenceQuery);
        resolution_test.testCompleteness();
        resolution_test.close();
    }

    @Test
    public void testCase5HappyPath() {
        Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case5", "schema.gql").toAbsolutePath();
        Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case5", "data.gql").toAbsolutePath();
        GraqlGet inferenceQuery = Graql.parse("match $c isa company, has name $n; get;").asGet();

        Session completeSession = graknTestServer.sessionWithNewKeyspace();
        Session testSession = graknTestServer.sessionWithNewKeyspace();
        Resolution resolution_test = new Resolution(completeSession, testSession);
        resolution_test.testQuery(inferenceQuery);
        resolution_test.testCompleteness();
        resolution_test.close();
    }

    @Test
    public void testCase6HappyPath() {
        Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case6", "schema.gql").toAbsolutePath();
        Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case6", "data.gql").toAbsolutePath();
        GraqlGet inferenceQuery = Graql.parse("" +
                "match $com isa company; " +
                "{$com has name \"the-company\";} or {$com has name \"another-company\";}; " +
                "not {$com has is-liable $liability;}; " +
                "get;").asGet();

        Session completeSession = graknTestServer.sessionWithNewKeyspace();
        Session testSession = graknTestServer.sessionWithNewKeyspace();
        Resolution resolution_test = new Resolution(completeSession, testSession);
        resolution_test.testQuery(inferenceQuery);
        resolution_test.testCompleteness();
        resolution_test.close();
    }

    @Test
    public void testCase7HappyPath() {
        Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case7", "schema.gql").toAbsolutePath();
        Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case7", "data.gql").toAbsolutePath();
        GraqlGet inferenceQuery = Graql.parse("" +
                "match $com isa company, has is-liable $lia; $lia true; get;").asGet();

        Session completeSession = graknTestServer.sessionWithNewKeyspace();
        Session testSession = graknTestServer.sessionWithNewKeyspace();
        Resolution resolution_test = new Resolution(completeSession, testSession);
        resolution_test.testQuery(inferenceQuery);
        resolution_test.testCompleteness();
        resolution_test.close();
    }
}

