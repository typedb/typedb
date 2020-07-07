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

import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.behaviour.resolution.framework.Resolution;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlQuery;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.function.Function;

import static grakn.core.test.behaviour.connection.ConnectionSteps.sessions;

public class ResolutionSteps {

    private Session reasonedSession;
    private Session materialisedSession;
    private GraqlGet queryToTest;
    private Resolution resolution;

    @Given("for each session, graql define")
    public void graql_define(String defineQueryStatements) {
        graql_query(defineQueryStatements, GraqlQuery::asDefine);
    }

    @Given("for each session, graql insert")
    public void graql_insert(String insertQueryStatements) {
        graql_query(insertQueryStatements, GraqlQuery::asInsert);
    }

    @Given("materialised keyspace is named: {string}")
    public void materialised_keyspace_is_named(final String keyspaceName) {
        final Session materialisedSession = sessions.stream()
                .filter(s -> s.keyspace().name().equals(keyspaceName))
                .findAny()
                .orElse(null);
        setMaterialisedSession(materialisedSession);
    }

    @Given("reasoned keyspace is named: {string}")
    public void reasoned_keyspace_is_named(final String keyspaceName) {
        final Session reasonedSession = sessions.stream()
                .filter(s -> s.keyspace().name().equals(keyspaceName))
                .findAny()
                .orElse(null);
        setReasonedSession(reasonedSession);
    }

    @When("materialised keyspace is completed")
    public void materialised_keyspace_is_completed() {
        resolution = new Resolution(getMaterialisedSession(), getReasonedSession());
    }

    @Then("for graql query")
    public void for_graql_query(String graqlQuery) {
        queryToTest = Graql.parse(graqlQuery).asGet();
    }

    @Then("in reasoned keyspace, answer size is: {number}")
    public void reasoned_keyspace_answer_size_is(final int expectedCount) {
        resolution.manuallyValidateAnswerSize(queryToTest, expectedCount);
    }

    @Then("in reasoned keyspace, all answers are correct")
    public void reasoned_keyspace_all_answers_are_correct() {
        // TODO: refactor these into a single method that compares the set of expected answers to the actual answers
        resolution.testQuery(queryToTest);
        resolution.testResolution(queryToTest);
    }

    @Then("materialised and reasoned keyspaces are the same size")
    public void keyspaces_are_the_same_size() {
        resolution.testCompleteness();
    }

    private <TQuery extends GraqlQuery> void graql_query(final String queryStatements, final Function<GraqlQuery, TQuery> queryTypeFn) {
        sessions.forEach(session -> {
            final Transaction tx = session.transaction(Transaction.Type.WRITE);
            final TQuery graqlQuery = queryTypeFn.apply(Graql.parse(String.join("\n", queryStatements)));
            tx.execute(graqlQuery, true, true); // always use inference and have explanations
            tx.commit();
        });
    }

    private Session getReasonedSession() {
        return reasonedSession;
    }

    private Session getMaterialisedSession() {
        return materialisedSession;
    }

    private void setReasonedSession(Session value) {
        reasonedSession = value;
    }

    private void setMaterialisedSession(Session value) {
        materialisedSession = value;
    }
}
