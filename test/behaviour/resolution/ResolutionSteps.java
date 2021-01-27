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

package grakn.core.test.behaviour.resolution;

import grakn.core.Grakn;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Options;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.test.behaviour.exception.ScenarioDefinitionException;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlMatch;
import graql.lang.query.GraqlQuery;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.List;
import java.util.Set;

import static grakn.core.test.behaviour.connection.ConnectionSteps.sessions;
import static grakn.core.test.behaviour.connection.ConnectionSteps.sessionsToTransactions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class ResolutionSteps {

    private Grakn.Session reasonedSession;
    private Grakn.Session materialisedSession;
    private GraqlMatch queryToTest;
    private Set<ConceptMap> answers;

    @Given("for each session, graql define")
    public void graql_define(String defineQueryStatements) {
        graql_query(defineQueryStatements);
    }

    @Given("for each session, graql insert")
    public void graql_insert(String insertQueryStatements) {
        graql_query(insertQueryStatements);
    }

    @Given("materialised session has database name: {word}")
    public void materialised_session_has_database_name(String databaseName) {
        Grakn.Session materialisedSession = sessions.stream()
                .filter(s -> s.database().name().equals(databaseName))
                .findAny()
                .orElse(null);
        this.materialisedSession = materialisedSession;
    }

    @Given("reasoned session has database name: {word}")
    public void reasoned_session_has_database_name(String databaseName) {
        Grakn.Session reasonedSession = sessions.stream()
                .filter(s -> s.database().name().equals(databaseName))
                .findAny()
                .orElse(null);
        this.reasonedSession = reasonedSession;
    }

    @When("materialised database is completed")
    public void materialised_database_is_completed() {
        // TODO
    }

    @Then("for graql query")
    public void for_graql_query(String graqlQuery) {
        queryToTest = Graql.parseQuery(graqlQuery).asMatch();
    }

    @Then("answer size in reasoned database is: {number}")
    public void answer_size_in_reasoned_database_is(int expectedCount) {
        int resultCount;
        Grakn.Transaction tx = getTransaction(reasonedSession);
        answers = tx.query().match(queryToTest).toSet();
        resultCount = answers.size();
        if (expectedCount != resultCount) {
            String msg = String.format("Query had an incorrect number of answers. Expected [%d] answers, " +
                                               "but found [%d] answers, for query :\n %s", expectedCount, resultCount, queryToTest);
            fail(msg);
        }
    }

    @Then("answers are consistent across {int} executions in reasoned database")
    public void answers_are_consistent_across_n_executions_in_reasoned_database(int executionCount) {
        Set<ConceptMap> oldAnswers;
        Grakn.Transaction tx = getTransaction(reasonedSession);
        oldAnswers = tx.query().match(queryToTest).toSet();
        for (int i = 0; i < executionCount - 1; i++) {
            try (Grakn.Transaction transaction = reasonedSession.transaction(Arguments.Transaction.Type.READ)) {
                Set<ConceptMap> answers = transaction.query().match(queryToTest).toSet();
                assertEquals(oldAnswers, answers);
            }
        }
    }

    @Then("answer set is equivalent for graql query")
    public void equivalent_answer_set(String equivalentQuery) {
        assertNotNull("A graql query must have been previously loaded in order to test answer equivalence.", queryToTest);
        assertNotNull("There are no previous answers to test against; was the reference query ever executed?", answers);
        Grakn.Transaction tx = getTransaction(reasonedSession);
        Set<ConceptMap> newAnswers = tx.query().match(Graql.parseQuery(equivalentQuery).asMatch()).toSet();
        assertEquals(answers, newAnswers);
    }

    @Then("all answers are correct in reasoned database")
    public void reasoned_database_all_answers_are_correct() {
        // TODO
    }

    @Then("materialised and reasoned databases are the same size")
    public void databases_are_the_same_size() {
        // TODO
    }

    private void graql_query(String queryStatements) {
        sessions.forEach(session -> {
            Grakn.Transaction tx = getTransaction(session);
            GraqlQuery query = Graql.parseQuery(String.join("\n", queryStatements));
            if (query instanceof GraqlMatch) {
                tx.query().match(query.asMatch());
            } else if (query instanceof GraqlInsert) {
                tx.query().insert(query.asInsert());
            } else if (query instanceof GraqlDefine) {
                tx.query().define(query.asDefine());
            } else {
                throw new ScenarioDefinitionException("Query not handled in ResolutionSteps" + queryStatements);
            }
        });
    }

    private Grakn.Transaction getTransaction(Grakn.Session session) {
        List<Grakn.Transaction> transactions = sessionsToTransactions.get(session);
        assert transactions.size() == 1;
        return transactions.get(0);
    }

}
