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

package grakn.core.test.behaviour.resolution.framework;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.behaviour.resolution.framework.complete.Completer;
import grakn.core.test.behaviour.resolution.framework.complete.SchemaManager;
import grakn.core.test.behaviour.resolution.framework.resolve.QueryBuilder;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static grakn.core.test.behaviour.resolution.framework.complete.SchemaManager.filterCompletionSchema;

public class Resolution {

    private Session completionSession;
    private Session testSession;
    private int completedInferredThingCount;
    private int initialThingCount;

    public Resolution(Session completionSession, Session testSession) {
        this.completionSession = completionSession;
        this.testSession = testSession;

        // TODO Check that nothing in the given schema conflicts with the resolution schema

        // Complete the KB-complete
        Completer completer = new Completer(this.completionSession);
        try (Transaction tx = this.completionSession.transaction(Transaction.Type.WRITE)) {
            completer.loadRules(SchemaManager.getAllRules(tx));
        }

        SchemaManager.undefineAllRules(this.completionSession);
        SchemaManager.enforceAllTypesHaveKeys(this.completionSession);
        SchemaManager.addResolutionSchema(this.completionSession);
        SchemaManager.connectResolutionSchema(this.completionSession);
        initialThingCount = thingCount(this.completionSession);
        completedInferredThingCount = completer.complete();
    }

    /**
     * Get a count of the number of instances in the KB
     * @param session Grakn Session
     * @return number of instances
     */
    private static int thingCount(Session session) {
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            return getOnlyElement(tx.execute(Graql.match(Graql.var("x").isa("thing")).get().count())).number().intValue();
        }
    }

    public void close() {
        completionSession.close();
        testSession.close();
    }

    /**
     * Run a query against the completion keyspace and the test keyspace and assert that they have the same number of
     * answers.
     * @param inferenceQuery The reference query to make against both keyspaces
     */
    public void testQuery(GraqlGet inferenceQuery) {
        Transaction testTx = testSession.transaction(Transaction.Type.READ);
        int testResultsCount = testTx.execute(inferenceQuery).size();
        testTx.close();

        Transaction completionTx = completionSession.transaction(Transaction.Type.READ);
        int completionResultsCount = filterCompletionSchema(completionTx.stream(inferenceQuery)).collect(Collectors.toSet()).size();
        completionTx.close();
        if (completionResultsCount != testResultsCount) {
            String msg = String.format("Query had an incorrect number of answers. Expected %d answers, but found %d " +
                    "answers, for query :\n %s", completionResultsCount, testResultsCount, inferenceQuery);
            throw new CorrectnessException(msg);
        }
    }

    /**
     * For each answer to a query, fully explore its explanation to construct a query that will check it was resolved
     * as expected. Run this query on the completion keyspace to verify.
     * @param inferenceQuery The reference query to make against both keyspaces
     */
    public void testResolution(GraqlGet inferenceQuery) {
        QueryBuilder rb = new QueryBuilder();
        List<GraqlGet> queries;

        try (Transaction tx = testSession.transaction(Transaction.Type.READ)) {
            queries = rb.buildMatchGet(tx, inferenceQuery);
        }

        if (queries.size() == 0) {
            String msg = String.format("No resolution queries were constructed for query %s", inferenceQuery);
            throw new CorrectnessException(msg);
        }

        try (Transaction tx = completionSession.transaction(Transaction.Type.READ)) {
            for (GraqlGet query: queries) {
                List<ConceptMap> answers = tx.execute(query);
                if (answers.size() != 1) {
                    String msg = String.format("Resolution query had %d answers, it should have had 1. The query is:\n %s", answers.size(), query);
                    throw new CorrectnessException(msg);
                }
            }
        }
    }

    /**
     * It is possible that rules could trigger when they should not. Testing for completeness checks the number of
     * inferred facts in the completion keyspace against the total number that are inferred in the test keyspace
     */
    public void testCompleteness() {
        int testInferredCount = thingCount(testSession) - initialThingCount;
        if (testInferredCount != completedInferredThingCount) {
            String msg = String.format("The complete KB contains %d inferred concepts, whereas the test KB contains %d inferred concepts.", completedInferredThingCount, testInferredCount);
            throw new CompletenessException(msg);
        }
    }

    public static class CorrectnessException extends RuntimeException {
        CorrectnessException(String message) {
            super(message);
        }
    }

    public static class CompletenessException extends RuntimeException {
        CompletenessException(String message) {
            super(message);
        }
    };
}
