package grakn.core.test.behaviour.resolution;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.behaviour.resolution.complete.Completer;
import grakn.core.test.behaviour.resolution.complete.SchemaManager;
import grakn.core.test.behaviour.resolution.resolve.QueryBuilder;
import graql.lang.query.GraqlGet;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static grakn.core.test.behaviour.resolution.common.Utils.loadGqlFile;
import static grakn.core.test.behaviour.resolution.common.Utils.thingCount;

public class Resolution {

    private final Path schemaPath;
    private final Path dataPath;
    private Session completionSession;
    private Session testSession;
    private int completedInferredThingCount;
    private int initialThingCount;

    public Resolution(Session completionSession, Session testSession, Path schemaPath, Path dataPath) {
        this.completionSession = completionSession;
        this.testSession = testSession;
        this.schemaPath = schemaPath;
        this.dataPath = dataPath;

        initialiseKeyspace(this.testSession);
        initialiseKeyspace(this.completionSession);

        // TODO Check that nothing in the given schema conflicts with the resolution schema
        // TODO Also check that all of the data in the initial data given has keys/ is uniquely identifiable

        // Complete the KB-complete
        Completer completer = new Completer(this.completionSession);
        try (Transaction tx = this.completionSession.transaction(Transaction.Type.WRITE)) {
            completer.loadRules(tx, SchemaManager.getAllRules(tx));
        }

        SchemaManager.undefineAllRules(this.completionSession);
        SchemaManager.enforceAllTypesHaveKeys(this.completionSession);
        SchemaManager.addResolutionSchema(this.completionSession);
        SchemaManager.connectResolutionSchema(this.completionSession);
        initialThingCount = thingCount(this.completionSession);
        completedInferredThingCount = completer.complete();
    }

    public void close() {
        completionSession.close();
        testSession.close();
    }

    public void testQuery(GraqlGet inferenceQuery) {
        QueryBuilder rb = new QueryBuilder();
        List<GraqlGet> queries;

        try (Transaction tx = testSession.transaction(Transaction.Type.READ)) {
            queries = rb.buildMatchGet(tx, inferenceQuery);
        }

        try (Transaction tx = completionSession.transaction(Transaction.Type.READ)) {
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
