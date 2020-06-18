package grakn.core.test.behaviour.resolution;

import grakn.client.GraknClient;
import grakn.client.GraknClient.Session;
import grakn.client.GraknClient.Transaction;
import grakn.client.answer.ConceptMap;
import grakn.verification.resolution.complete.Completer;
import grakn.verification.resolution.complete.SchemaManager;
import grakn.verification.resolution.resolve.QueryBuilder;
import graql.lang.query.GraqlGet;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static grakn.verification.resolution.common.Utils.loadGqlFile;
import static grakn.verification.resolution.common.Utils.thingCount;

public class Resolution {

    private static final String COMPLETE_KEYSPACE = "complete";
    private static final String TEST_KEYSPACE = "test";
    private static GraknClient graknClient;
    private final Path schemaPath;
    private final Path dataPath;
    private Session completeSession;
    private Session testSession;
    private int completedInferredThingCount;
    private int initialThingCount;

    public Resolution(String graknUri, Path schemaPath, Path dataPath) {
        this.schemaPath = schemaPath;
        this.dataPath = dataPath;
        graknClient = new GraknClient(graknUri);

        testSession = graknClient.session(TEST_KEYSPACE);
        completeSession = graknClient.session(COMPLETE_KEYSPACE);

        initialiseKeyspace(testSession);
        initialiseKeyspace(completeSession);

        // TODO Check that nothing in the given schema conflicts with the resolution schema
        // TODO Also check that all of the data in the initial data given has keys/ is uniquely identifiable

        // Complete the KB-complete
        Completer completer = new Completer(completeSession);
        try (Transaction tx = completeSession.transaction().write()) {
            completer.loadRules(tx, SchemaManager.getAllRules(tx));
        }

        SchemaManager.undefineAllRules(completeSession);
        SchemaManager.enforceAllTypesHaveKeys(completeSession);
        SchemaManager.addResolutionSchema(completeSession);
        SchemaManager.connectResolutionSchema(completeSession);
        initialThingCount = thingCount(completeSession);
        completedInferredThingCount = completer.complete();
    }

    public void close() {
        completeSession.close();
        testSession.close();
        graknClient.keyspaces().delete(COMPLETE_KEYSPACE);
        graknClient.keyspaces().delete(TEST_KEYSPACE);
    }

    public void testQuery(GraqlGet inferenceQuery) {
        QueryBuilder rb = new QueryBuilder();
        List<GraqlGet> queries;

        try (Transaction tx = testSession.transaction().read()) {
            queries = rb.buildMatchGet(tx, inferenceQuery);
        }

        try (Transaction tx = completeSession.transaction().read()) {
            for (GraqlGet query: queries) {
                testResolution(tx, query);
            }
        }
    }

    public void testCompleteness() {
        int testInferredCount = thingCount(testSession) - initialThingCount;
        if (testInferredCount != completedInferredThingCount) {
            String msg = String.format("The complete KB contains %d inferred concepts, whereas the test KB contains %d inferred concepts.", completedInferredThingCount, testInferredCount);
            throw new RuntimeException(msg);
        }
    }

    private void testResolution(Transaction tx, GraqlGet query) {
        List<ConceptMap> answers = tx.execute(query);
        if (answers.size() != 1) {
            String msg = String.format("Resolution query had %d answers, it should have had 1. The query is:\n %s", answers.size(), query);
            throw new RuntimeException(msg);
        }
    }

    private void initialiseKeyspace(Session session) {
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
}
