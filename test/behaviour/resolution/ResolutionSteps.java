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

import static grakn.core.test.behaviour.connection.ConnectionSteps.sessions;

public class ResolutionSteps {

    private Session reasonedSession;
    private Session materialisedSession;
    private GraqlGet query;
    private Resolution resolution;

    @Given("graql define")
    public void graql_define(String defineQueryStatements) {
        sessions.forEach(session -> {
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            GraqlDefine graqlQuery = Graql.parse(String.join("\n", defineQueryStatements)).asDefine();
            tx.execute(graqlQuery);
            tx.commit();
        });
    }

    @Given("graql insert")
    public void graql_insert(String insertQueryStatements) {
        sessions.forEach(session -> {
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            GraqlQuery graqlQuery = Graql.parse(String.join("\n", insertQueryStatements));
            tx.execute(graqlQuery, true, true); // always use inference and have explanations
            tx.commit();
        });
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

    @When("reference kb is completed")
    public void reference_kb_is_completed() {
        resolution = new Resolution(getMaterialisedSession(), getReasonedSession());
    }

    @Then("for graql query")
    public void for_graql_query(String graqlQuery) {
        query = Graql.parse(graqlQuery);
    }

    @Then("for reasoned keyspace, answer size is: {number}")
    public void answer_count_is(final int expectedCount) {
        resolution.manuallyValidateAnswerSize(query, expectedCount);
    }

    @Then("answer count is correct")
    public void answer_count_is_correct() {
        resolution.testQuery(query);
    }

    @Then("answers resolution is correct")
    public void answers_resolution_is_correct() {
        resolution.testResolution(query);
    }

    @Then("test keyspace is complete")
    public void test_keyspace_is_complete() {
        resolution.testCompleteness();
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
