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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Arguments.Transaction.Type;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.CompletionSchema;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.reference.Reasoner;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.soundness.SoundnessChecker;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import com.vaticle.typeql.lang.query.TypeQLQuery;

import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.core.TypeDB.Transaction;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class Resolution {

    private final RocksSession session;
    private final Reasoner referenceReasoner;

    /**
     * Resolution Testing Framework's entry point. Takes in sessions each for a `Completion` and `Test` keyspace. Each
     * keyspace loaded with the same schema and data. This should be true unless testing this code, in which case a
     * disparity between the two keyspaces is introduced to check that the framework throws an error when it should.
     * @param session TypeDB session where, expects schema (inlc. rules) and data to be already present
     */
    public Resolution(RocksSession session) {
        this.session = session;
        this.referenceReasoner = new Reasoner();
        this.referenceReasoner.run(this.session);
    }

    public void close() {
        session.close();
    }

    /**
     * Run a query against the materialised keyspace and the reasoned keyspace and assert that they have the same number
     * of answers.
     * @param inferenceQuery The reference query to make against both keyspaces
     */
    public void testQuery(TypeQLMatch inferenceQuery) {
        Transaction reasonedTx = session.transaction(Type.READ);
        int testResultsCount = reasonedTx.query().match(inferenceQuery).toList().size();
        reasonedTx.close();

        Transaction completionTx = session.transaction(Type.READ);
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
        Set<String> completionSchemaTypes = iterate(CompletionSchema.CompletionSchemaType.values())
                .map(CompletionSchema.CompletionSchemaType::toString).toSet();
        return answerStream.filter(a -> iterate(a.concepts().values())
                .noneMatch(concept -> completionSchemaTypes
                        .contains(concept.asThing().getType().getLabel().toString())));
    }

    /**
     * For each answer to a query, fully explore its explanation to construct a query that will check it was resolved
     * as expected. Run this query on the completion keyspace to verify.
     * @param inferenceQuery The reference query to make against both keyspaces
     */
    public void testSoundness(TypeQLMatch inferenceQuery) {

        List<TypeQLMatch> queries;

        try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
            SoundnessChecker soundnessChecker = new SoundnessChecker(referenceReasoner, tx);
            soundnessChecker.check(inferenceQuery);
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
