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
import grakn.core.test.behaviour.connection.session.SessionManager;
import grakn.core.test.behaviour.resolution.framework.Resolution;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.Arrays;

import static grakn.core.test.behaviour.connection.ConnectionSteps.sessions;

public class ResolutionSteps {

    private SessionManager sessionManager;
    private static GraqlGet query;
    private static Resolution resolution;

    @Before
    public void testSetup() {
        sessionManager = new SessionManager();
    }

    @After
    public void cleanup() {
        resolution.close();
    }

    @When("reference kb is completed")
    public void reference_kb_is_completed() {
        final Session completionSession = sessions.stream()
                .filter(s -> s.keyspace().name().equalsIgnoreCase("completion"))
                .findAny()
                .orElse(null);
        final Session testSession = sessions.stream()
                .filter(s -> s.keyspace().name().equalsIgnoreCase("test"))
                .findAny()
                .orElse(null);
        sessionManager.createSessions(Arrays.asList(completionSession, testSession));
        resolution = new Resolution(sessionManager);
    }

    @Then("for graql query")
    public void for_graql_query(String graqlQuery) {
        query = Graql.parse(graqlQuery);
    }

    @Then("answer count is: {number}")
    public void answer_count_is(final int expectedCount) {
        resolution.manuallyTestQuery(query, expectedCount);
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
}
