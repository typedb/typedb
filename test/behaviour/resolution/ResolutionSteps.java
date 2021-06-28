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

package com.vaticle.typedb.core.test.behaviour.resolution;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.test.behaviour.resolution.correctness.CorrectnessChecker;
import com.vaticle.typedb.core.test.behaviour.typeql.TypeQLSteps;
import com.vaticle.typeql.lang.TypeQL;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;

import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.sessions;
import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.sessionsToTransactions;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

public class ResolutionSteps {

    private CorrectnessChecker correctnessChecker;

    static RocksSession session() {
        return sessions.get(0);
    }

    static RocksTransaction tx() {
        return sessionsToTransactions.get(sessions.get(0)).get(0);
    }

    @Given("correctness checker is initialised")
    public void correctness_checker_is_initialised() {
        correctnessChecker = CorrectnessChecker.initialise(session());
    }

    @Then("answers are consistent across {int} executions")
    public static void answers_are_consistent_across_n_executions_in_reasoned_database(int executionCount) {
        Set<ConceptMap> oldAnswers;
        oldAnswers = tx().query().match(TypeQLSteps.typeQLQuery).toSet();
        for (int i = 0; i < executionCount - 1; i++) {
            try (TypeDB.Transaction transaction = session().transaction(Arguments.Transaction.Type.READ,
                                                                                new Options.Transaction().infer(true))) {
                Set<ConceptMap> answers = transaction.query().match(TypeQLSteps.typeQLQuery).toSet();
                assertEquals(oldAnswers, answers);
            }
        }
    }

    @Then("answer set is equivalent for typeql query")
    public static void equivalent_answer_set(String equivalentQuery) {
        assertNotNull("A typeql query must have been previously loaded in order to test answer equivalence.", TypeQLSteps.typeQLQuery);
        assertNotNull("There are no previous answers to test against; was the reference query ever executed?", TypeQLSteps.answers);
        Set<ConceptMap> newAnswers = tx().query().match(TypeQL.parseQuery(equivalentQuery).asMatch()).toSet();
        assertEquals(set(TypeQLSteps.answers), newAnswers);
    }

    @Then("check all answers and explanations are correct")
    public void check_all_answers_and_explanations_are_correct() {
        correctnessChecker.checkSoundness(TypeQLSteps.typeQLQuery);
        correctnessChecker.checkCompleteness(TypeQLSteps.typeQLQuery);
    }

    @Then("check all answers and explanations are sound")
    public void check_all_answers_and_explanations_are_sound() {
        correctnessChecker.checkSoundness(TypeQLSteps.typeQLQuery);
    }

    @Then("check all answers and explanations are complete")
    public void check_all_answers_and_explanations_are_complete() {
        correctnessChecker.checkCompleteness(TypeQLSteps.typeQLQuery);
    }

}
