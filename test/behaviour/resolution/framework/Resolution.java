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
import grakn.core.test.behaviour.resolution.framework.resolve.ResolutionQueryBuilder;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlQuery;

import java.util.List;
import java.util.stream.Collectors;

import static grakn.core.test.behaviour.resolution.framework.complete.SchemaManager.filterCompletionSchema;

public class Resolution {

    private Session materialisedSession;
    private Session reasonedSession;

    /**
     * Resolution Testing Framework's entry point. Takes in sessions each for a `Completion` and `Test` keyspace. Each
     * keyspace loaded with the same schema and data. This should be true unless testing this code, in which case a
     * disparity between the two keyspaces is introduced to check that the framework throws an error when it should.
     * @param materialisedSession a session for the `materialised` keyspace with base data included
     * @param reasonedSession a session for the `reasoned` keyspace with base data included
     */
    public Resolution(Session materialisedSession, Session reasonedSession) {
        this.materialisedSession = materialisedSession;
        this.reasonedSession = reasonedSession;

        // TODO Check that nothing in the given schema conflicts with the resolution schema

        // Complete the KB-complete
        Completer completer = new Completer(this.materialisedSession);
        try (Transaction tx = this.materialisedSession.transaction(Transaction.Type.WRITE)) {
            completer.loadRules(SchemaManager.getAllRules(tx));
        }

        SchemaManager.undefineAllRules(this.materialisedSession);
        SchemaManager.addResolutionSchema(this.materialisedSession);
        SchemaManager.connectResolutionSchema(this.materialisedSession);
        completer.complete();
    }

    public void close() {
        materialisedSession.close();
        reasonedSession.close();
    }

    /**
     * Run a query against the materialised keyspace and the reasoned keyspace and assert that they have the same number
     * of answers.
     * @param inferenceQuery The reference query to make against both keyspaces
     */
    public void testQuery(GraqlGet inferenceQuery) {
        Transaction reasonedTx = reasonedSession.transaction(Transaction.Type.READ);
        int testResultsCount = reasonedTx.execute(inferenceQuery).size();
        reasonedTx.close();

        Transaction completionTx = materialisedSession.transaction(Transaction.Type.READ);
        int completionResultsCount = filterCompletionSchema(completionTx.stream(inferenceQuery)).collect(Collectors.toSet()).size();
        completionTx.close();
        if (completionResultsCount != testResultsCount) {
            throw new WrongAnswerSizeException(completionResultsCount, testResultsCount, inferenceQuery);
        }
    }

    /**
     * For each answer to a query, fully explore its explanation to construct a query that will check it was resolved
     * as expected. Run this query on the completion keyspace to verify.
     * @param inferenceQuery The reference query to make against both keyspaces
     */
    public void testResolution(GraqlGet inferenceQuery) {
        ResolutionQueryBuilder resolutionQueryBuilder = new ResolutionQueryBuilder();
        List<GraqlGet> queries;

        try (Transaction tx = reasonedSession.transaction(Transaction.Type.READ)) {
            queries = resolutionQueryBuilder.buildMatchGet(tx, inferenceQuery);
        }

        if (queries.size() == 0) {
            String msg = String.format("No resolution queries were constructed for query %s", inferenceQuery);
            throw new CorrectnessException(msg);
        }

        try (Transaction tx = materialisedSession.transaction(Transaction.Type.READ)) {
            for (GraqlGet query: queries) {
                List<ConceptMap> answers = tx.execute(query);
                if (answers.isEmpty()) {
                    String msg = String.format("Resolution query had %d answers, it should have had some. The query is:\n %s", answers.size(), query);
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
        try {
            testQuery(Graql.parse("match $x isa thing; get;").asGet());
            testQuery(Graql.parse("match $r ($x) isa relation; get;").asGet());
            testQuery(Graql.parse("match $x has attribute $y; get;").asGet());
        } catch (WrongAnswerSizeException ex) {
            String msg = String.format("Failed completeness test: [%s]. The complete KB contains %d inferred concepts, whereas the test KB contains %d inferred concepts.",
                    ex.getInferenceQuery(), ex.getExpectedAnswers(), ex.getActualAnswers());
            throw new CompletenessException(msg);
        }
    }

    public static class CorrectnessException extends RuntimeException {
        public CorrectnessException(String message) {
            super(message);
        }
    }

    public static class CompletenessException extends RuntimeException {
        CompletenessException(String message) {
            super(message);
        }
    }

    public static class WrongAnswerSizeException extends RuntimeException {
        private final int expectedAnswers;
        private final int actualAnswers;
        private final GraqlQuery inferenceQuery;

        public WrongAnswerSizeException(final int expectedAnswers, final int actualAnswers, final GraqlQuery inferenceQuery) {
            super(String.format("Query had an incorrect number of answers. Expected %d answers, but found %d " +
                    "answers, for query :\n %s", expectedAnswers, actualAnswers, inferenceQuery));
            this.actualAnswers = actualAnswers;
            this.expectedAnswers = expectedAnswers;
            this.inferenceQuery = inferenceQuery;
        }

        public int getActualAnswers() {
            return actualAnswers;
        }

        public int getExpectedAnswers() {
            return expectedAnswers;
        }

        public GraqlQuery getInferenceQuery() {
            return inferenceQuery;
        }
    }
}
