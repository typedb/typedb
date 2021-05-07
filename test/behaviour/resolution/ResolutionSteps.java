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
import com.vaticle.typedb.core.test.behaviour.exception.ScenarioDefinitionException;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import com.vaticle.typeql.lang.query.TypeQLQuery;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.sessions;
import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.sessionsToTransactions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class ResolutionSteps {

    private static final String MATERIALISED_DATABASE = "materialised";
    private static final String REASONED_DATABASE = "reasoned";

    private TypeQLMatch queryToTest;
    private Set<ConceptMap> answers;

    @Given("for each session, typeql define")
    public void typeql_define(String defineQueryStatements) {
        typeql_query(defineQueryStatements);
    }

    @Given("for each session, typeql insert")
    public void typeql_insert(String insertQueryStatements) {
        typeql_query(insertQueryStatements);
    }

    @When("materialised database is completed")
    public void materialised_database_is_completed() {
        // TODO
    }

    @Then("for typeql query")
    public void for_typeql_query(String typeQLQuery) {
        queryToTest = TypeQL.parseQuery(typeQLQuery).asMatch();
    }

    @Then("answer size in reasoned database is: {number}")
    public void answer_size_in_reasoned_database_is(int expectedCount) {
        int resultCount;
        TypeDB.Transaction tx = reasonedDbTxn();
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
        TypeDB.Transaction tx = reasonedDbTxn();
        oldAnswers = tx.query().match(queryToTest).toSet();
        for (int i = 0; i < executionCount - 1; i++) {
            try (TypeDB.Transaction transaction = reasonedSession().transaction(Arguments.Transaction.Type.READ,
                                                                                new Options.Transaction().infer(true))) {
                Set<ConceptMap> answers = transaction.query().match(queryToTest).toSet();
                assertEquals(oldAnswers, answers);
            }
        }
    }

    @Then("answer set is equivalent for typeql query")
    public void equivalent_answer_set(String equivalentQuery) {
        assertNotNull("A typeql query must have been previously loaded in order to test answer equivalence.", queryToTest);
        assertNotNull("There are no previous answers to test against; was the reference query ever executed?", answers);
        TypeDB.Transaction tx = reasonedDbTxn();
        Set<ConceptMap> newAnswers = tx.query().match(TypeQL.parseQuery(equivalentQuery).asMatch()).toSet();
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

    private void typeql_query(String queryStatements) {
        sessions.forEach(session -> {
            TypeDB.Transaction tx = getTransaction(session);
            TypeQLQuery query = TypeQL.parseQuery(String.join("\n", queryStatements));
            if (query instanceof TypeQLMatch) {
                tx.query().match(query.asMatch());
            } else if (query instanceof TypeQLInsert) {
                tx.query().insert(query.asInsert());
            } else if (query instanceof TypeQLDefine) {
                tx.query().define(query.asDefine());
            } else {
                throw new ScenarioDefinitionException("Query not handled in ResolutionSteps" + queryStatements);
            }
        });
    }

    private TypeDB.Session reasonedSession() {
        TypeDB.Session reasonedSession = sessions.stream()
                .filter(s -> s.database().name().equals(REASONED_DATABASE))
                .findAny()
                .orElse(null);
        assert reasonedSession != null : "Reasoned session with database name " + REASONED_DATABASE + " does not exist";
        return reasonedSession;
    }

    private TypeDB.Session materialisedSession() {
        TypeDB.Session materialisedSession = sessions.stream()
                .filter(s -> s.database().name().equals(MATERIALISED_DATABASE))
                .findAny()
                .orElse(null);
        assert materialisedSession != null : "Materialised session with database name " + MATERIALISED_DATABASE + " does not exist";
        return materialisedSession;
    }

    private TypeDB.Transaction reasonedDbTxn() {
        return getTransaction(reasonedSession());
    }

    private TypeDB.Transaction materialisedDbTxn() {
        return getTransaction(materialisedSession());
    }

    private TypeDB.Transaction getTransaction(TypeDB.Session session) {
        List<TypeDB.Transaction> transactions = sessionsToTransactions.get(session);
        assert transactions.size() == 1;
        return transactions.get(0);
    }

}
