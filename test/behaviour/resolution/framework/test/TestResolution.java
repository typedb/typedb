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

package com.vaticle.typedb.core.test.behaviour.resolution.framework.test;

import com.vaticle.typedb.core.TypeDB.Session;
import com.vaticle.typedb.core.TypeDB.Transaction;;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.Resolution;
import com.vaticle.typedb.core.test.rule.GraknTestServer;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import static com.vaticle.typedb.core.test.behaviour.resolution.framework.test.LoadTest.loadTestStub;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class TestResolution {


    @ClassRule
    public static final GraknTestServer graknTestServer = new GraknTestServer();

    private void resolutionHappyPathTest(String stubName, TypeQLMatch inferenceQuery) {
        Session completionSession = graknTestServer.sessionWithNewKeyspace();
        loadTestStub(completionSession, stubName);

        Session testSession = graknTestServer.sessionWithNewKeyspace();
        loadTestStub(testSession, stubName);

        Resolution resolution_test = new Resolution(completionSession, testSession);
        resolution_test.testQuery(inferenceQuery);
        resolution_test.testResolution(inferenceQuery);
        resolution_test.testCompleteness();
        resolution_test.close();
    }

    // TODO: re-enable when 3-hop transitivity is resolvable
    @Test
    @Ignore
    public void testResolutionPassesForTransitivity() {
        TypeQLMatch inferenceQuery = TypeQL.parse("" +
                "match $lh (location-hierarchy_superior: $continent, " +
                "location-hierarchy_subordinate: $area) isa location-hierarchy; " +
                "$continent isa continent; " +
                "$area isa area;").asMatch();
        resolutionHappyPathTest("transitivity", inferenceQuery);
    }

    @Test
    public void testResolutionThrowsForTransitivityWhenRuleIsNotTriggered() {
        TypeQLMatch inferenceQuery = TypeQL.parse("" +
                "match $lh (location-hierarchy_superior: $continent, " +
                "location-hierarchy_subordinate: $area) isa location-hierarchy; " +
                "$continent isa continent; " +
                "$area isa area;").asMatch();


        Session completionSession = graknTestServer.sessionWithNewKeyspace();
        loadTestStub(completionSession, "transitivity");

        Session testSession = graknTestServer.sessionWithNewKeyspace();
        loadTestStub(testSession, "transitivity");

        // Undefine a rule in the keyspace under test such that the expected facts will not be inferred
        Transaction tx = testSession.transaction(Arguments.Transaction.Type.WRITE);
        tx.execute(TypeQL.undefine(TypeQL.type("location-hierarchy-transitivity").sub("rule")));
        tx.commit();

        Resolution resolution_test = new Resolution(completionSession, testSession);

        Exception testQueryThrows = null;
        try {
            resolution_test.testQuery(inferenceQuery);
        } catch (Exception e) {
            testQueryThrows = e;
        }
        assertNotNull(testQueryThrows);
        assertThat(testQueryThrows, instanceOf(Resolution.WrongAnswerSizeException.class));

        Exception testResolutionThrows = null;
        try {
            resolution_test.testResolution(inferenceQuery);
        } catch (Exception e) {
            testResolutionThrows = e;
        }
        assertNotNull(testResolutionThrows);
        assertThat(testResolutionThrows, instanceOf(Resolution.CorrectnessException.class));

        Exception testCompletenessThrows = null;
        try {
            resolution_test.testCompleteness();
        } catch (Exception e) {
            testCompletenessThrows = e;
        }
        assertNotNull(testCompletenessThrows);
        assertThat(testCompletenessThrows, instanceOf(Resolution.CompletenessException.class));

        resolution_test.close();
    }

    @Test
    public void testResolutionThrowsForTransitivityWhenRuleTriggersTooOften() {
        TypeQLMatch inferenceQuery = TypeQL.parse("" +
                "match $lh (location-hierarchy_superior: $continent, " +
                "location-hierarchy_subordinate: $area) isa location-hierarchy; " +
                "$continent isa continent; " +
                "$area isa area;").asMatch();


        Session completionSession = graknTestServer.sessionWithNewKeyspace();
        loadTestStub(completionSession, "transitivity");

        Session testSession = graknTestServer.sessionWithNewKeyspace();
        loadTestStub(testSession, "transitivity");

        // Undefine a rule in the keyspace under test such that the expected facts will not be inferred
        Transaction tx = testSession.transaction(Arguments.Transaction.Type.WRITE);
        tx.execute(TypeQL.undefine(TypeQL.type("location-hierarchy-transitivity").sub("rule")));
        tx.execute(TypeQL.define(TypeQL.parsePattern("location-hierarchy-transitivity sub rule,\n" +
                "when {\n" +
                "  ($a, $b) isa location-hierarchy;\n" +
                "  ($b, $c) isa location-hierarchy;\n" +
                "  $a != $c;\n" +
                "}, then {\n" +
                "  (location-hierarchy_superior: $a, location-hierarchy_subordinate: $c) isa location-hierarchy;\n" +
                "};").statements()));
        tx.commit();

        Resolution resolution_test = new Resolution(completionSession, testSession);

        resolution_test.testQuery(inferenceQuery);

        // In this case we ignore `testResolution()` as it could be correct or incorrect, since it could pick a correct
        // path, or one that has the location-hierarchy backwards

        Exception testCompletenessThrows = null;
        try {
            resolution_test.testCompleteness();
        } catch (Exception e) {
            testCompletenessThrows = e;
        }
        assertNotNull(testCompletenessThrows);
        assertThat(testCompletenessThrows, instanceOf(Resolution.CompletenessException.class));

        resolution_test.close();
    }

    @Test
    public void testResolutionThrowsForTransitivityWhenRuleTriggersTooOftenAndResultCountIsIncorrect() {
        TypeQLMatch inferenceQuery = TypeQL.parse("" +
                "match $lh ($continent, " +
                "$area) isa location-hierarchy; " +
                "$continent isa continent; " +
                "$area isa area;").asMatch();


        Session completionSession = graknTestServer.sessionWithNewKeyspace();
        loadTestStub(completionSession, "transitivity");

        Session testSession = graknTestServer.sessionWithNewKeyspace();
        loadTestStub(testSession, "transitivity");

        // Undefine a rule in the keyspace under test such that the expected facts will not be inferred
        Transaction tx = testSession.transaction(Arguments.Transaction.Type.WRITE);
        tx.execute(TypeQL.undefine(TypeQL.type("location-hierarchy-transitivity").sub("rule")));
        tx.execute(TypeQL.define(TypeQL.parsePattern("location-hierarchy-transitivity sub rule,\n" +
                "when {\n" +
                "  ($a, $b) isa location-hierarchy;\n" +
                "  ($b, $c) isa location-hierarchy;\n" +
                "  $a != $c;\n" +
                "}, then {\n" +
                "  (location-hierarchy_superior: $a, location-hierarchy_subordinate: $c) isa location-hierarchy;\n" +
                "};").statements()));
        tx.commit();

        Resolution resolution_test = new Resolution(completionSession, testSession);

        Exception testQueryThrows = null;
        try {
            resolution_test.testQuery(inferenceQuery);
        } catch (Exception e) {
            testQueryThrows = e;
        }
        assertNotNull(testQueryThrows);
        assertThat(testQueryThrows, instanceOf(Resolution.WrongAnswerSizeException.class));

        // In this case we ignore `testResolution()` as it could be correct or incorrect, since it could pick a correct
        // path, or one that has the location-hierarchy backwards

        Exception testCompletenessThrows = null;
        try {
            resolution_test.testCompleteness();
        } catch (Exception e) {
            testCompletenessThrows = e;
        }
        assertNotNull(testCompletenessThrows);
        assertThat(testCompletenessThrows, instanceOf(Resolution.CompletenessException.class));

        resolution_test.close();
    }

    @Test
    public void testResolutionPassesForTwoRecursiveRules() {
        TypeQLMatch inferenceQuery = TypeQL.parse("match $transaction has currency $currency;").asMatch();
        resolutionHappyPathTest("complex_recursion", inferenceQuery);
    }

    @Test
    public void testBasicRecursion() {
        TypeQLMatch inferenceQuery = TypeQL.parse("match $com isa company, has is-liable $lia;").asMatch();
        resolutionHappyPathTest("basic_recursion", inferenceQuery);
    }
}

