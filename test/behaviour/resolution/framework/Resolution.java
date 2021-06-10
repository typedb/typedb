/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.test.behaviour.resolution.framework;

import com.vaticle.typedb.core.TypeDB.Session;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Arguments.Transaction.Type;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.Completer;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.resolve.ResolutionQueryBuilder;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import com.vaticle.typeql.lang.query.TypeQLQuery;

import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.core.TypeDB.Transaction;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class Resolution {

    private final Session materialisedSession;
    private final Session reasonedSession;

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
        try (Transaction tx = this.materialisedSession.transaction(Type.WRITE)) {
            completer.loadRules(SchemaManager.getAllRules(tx));
        }

        SchemaManager.undefineAllRules(this.materialisedSession);
        SchemaManager.addCompletionSchema(this.materialisedSession);
        SchemaManager.connectCompletionSchema(this.materialisedSession);
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
    public void testQuery(TypeQLMatch inferenceQuery) {
        Transaction reasonedTx = reasonedSession.transaction(Type.READ);
        int testResultsCount = reasonedTx.query().match(inferenceQuery).toList().size();
        reasonedTx.close();

        Transaction completionTx = materialisedSession.transaction(Type.READ);
        // TODO: Consider negating the completion schema instead of filtering
        int completionResultsCount = filterCompletionSchema(completionTx.query().match(inferenceQuery)).toSet().size();
        completionTx.close();
        if (completionResultsCount != testResultsCount) {
            throw new WrongAnswerSizeException(completionResultsCount, testResultsCount, inferenceQuery);
        }
    }

    /**
     * Filters out answers that contain any Thing instance that is derived from the completion schema.
     * @param answerStream stream of answers to filter
     * @return filtered stream of answers
     */
    public static FunctionalIterator<ConceptMap> filterCompletionSchema(FunctionalIterator<ConceptMap> answerStream) {
        Set<String> completionSchemaTypes = iterate(SchemaManager.CompletionSchemaType.values())
                .map(SchemaManager.CompletionSchemaType::toString).toSet();
        return answerStream.filter(a -> iterate(a.concepts().values())
                .noneMatch(concept -> completionSchemaTypes
                        .contains(concept.asThing().getType().getLabel().toString())));
    }

    /**
     * For each answer to a query, fully explore its explanation to construct a query that will check it was resolved
     * as expected. Run this query on the completion keyspace to verify.
     * @param inferenceQuery The reference query to make against both keyspaces
     */
    public void testResolution(TypeQLMatch inferenceQuery) {
        ResolutionQueryBuilder resolutionQueryBuilder = new ResolutionQueryBuilder();
        List<TypeQLMatch> queries;

        try (Transaction tx = reasonedSession.transaction(Arguments.Transaction.Type.READ)) {
            queries = resolutionQueryBuilder.buildMatchGet(tx, inferenceQuery);
        }

        if (queries.size() == 0) {
            String msg = String.format("No resolution queries were constructed for query %s", inferenceQuery);
            throw new CorrectnessException(msg);
        }

        try (Transaction tx = materialisedSession.transaction(Arguments.Transaction.Type.READ)) {
            for (TypeQLMatch query: queries) {
                List<ConceptMap> answers = tx.query().match(query).toList();
                if (answers.isEmpty()) {
                    String msg = String.format("Resolution query found no answers, it should have had one or more " +
                                                       "for query:\n %s", query);
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
            testQuery(TypeQL.parseQuery("match $x isa thing;").asMatch());
            testQuery(TypeQL.parseQuery("match $r ($x) isa relation;").asMatch());
            testQuery(TypeQL.parseQuery("match $x has attribute $y;").asMatch());
        } catch (WrongAnswerSizeException ex) {
            String msg = String.format("Failed completeness test: [%s]. The complete database contains %d inferred " +
                                               "concepts, whereas the test database contains %d inferred concepts.",
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
        private final TypeQLQuery inferenceQuery;

        public WrongAnswerSizeException(final int expectedAnswers, final int actualAnswers, final TypeQLQuery inferenceQuery) {
            super(String.format("Query had an incorrect number of answers. Expected %d answers, but found %d " +
                    "answers for query:\n %s", expectedAnswers, actualAnswers, inferenceQuery));
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

        public TypeQLQuery getInferenceQuery() {
            return inferenceQuery;
        }
    }
}
