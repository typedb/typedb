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

package grakn.core.test.behaviour.resolution.framework.test;

import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.behaviour.resolution.framework.complete.SchemaManager;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.test.behaviour.resolution.framework.complete.SchemaManager.addResolutionSchema;
import static grakn.core.test.behaviour.resolution.framework.complete.SchemaManager.connectResolutionSchema;
import static grakn.core.test.behaviour.resolution.framework.test.LoadTest.loadTestStub;
import static org.junit.Assert.assertEquals;

public class TestSchemaManager {

    private static final String KEYSPACE = "test_schema_manager";

    @ClassRule
    public static final GraknTestServer graknTestServer = new GraknTestServer();

    @BeforeClass
    public static void beforeClass() {
        loadTestStub(graknTestServer.session(KEYSPACE), "complex_recursion");
    }

    @Test
    public void testResolutionSchemaRolesPlayedAreCorrect() {

        try (Session session = graknTestServer.session(KEYSPACE)) {

            addResolutionSchema(session);
            connectResolutionSchema(session);

            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {

                GraqlGet roleplayersQuery = Graql.match(
                        Graql.var("x").plays("instance"),
                        Graql.var("x").plays("owner"),
                        Graql.var("x").plays("roleplayer")
                ).get();

                Set<String> roleplayers = tx.stream(roleplayersQuery).map(r -> r.get("x").asType().label().toString()).collect(Collectors.toSet());

                HashSet<String> expectedRoleplayers = new HashSet<String>() {
                    {
                        add("currency");
                        add("country-name");
                        add("city-name");
                        add("transaction");
                        add("locates");
                        add("location-hierarchy");
                        add("location");
                        add("country");
                        add("city");
                    }
                };

                assertEquals(expectedRoleplayers, roleplayers);
            }
        }

    }

    @Test
    public void testResolutionSchemaRelationRolePlayedIsCorrect() {

        try (Session session = graknTestServer.session(KEYSPACE)) {

            addResolutionSchema(session);
            connectResolutionSchema(session);

            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {

                GraqlGet roleplayersQuery = Graql.match(
                        Graql.var("x").plays("rel")
                ).get();

                Set<String> roleplayers = tx.stream(roleplayersQuery).map(r -> r.get("x").asType().label().toString()).collect(Collectors.toSet());

                HashSet<String> expectedRoleplayers = new HashSet<String>() {
                    {
                        add("locates");
                        add("location-hierarchy");
                    }
                };
                assertEquals(expectedRoleplayers, roleplayers);
            }
        }

    }

    @Test
    public void testResolutionSchemaAttributesOwnedAreCorrect() {

        try (Session session = graknTestServer.session(KEYSPACE)) {

            addResolutionSchema(session);
            connectResolutionSchema(session);

            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {

                GraqlGet clauseAttributesQuery = Graql.match(Graql.var("x").sub("has-attribute-property")).get();

                Set<String> roles = tx.execute(clauseAttributesQuery).get(0).get("x").asRelationType().roles().map(a -> a.label().toString()).collect(Collectors.toSet());

                HashSet<String> expectedRoles = new HashSet<String>() {
                    {
                        add("owned");
                        add("owner");
                    }
                };

                assertEquals(expectedRoles, roles);
            }
        }
    }

    @Test
    public void testGetAllRulesReturnsExpectedRules() {
        try (Session session = graknTestServer.session(KEYSPACE)) {
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                Set<Rule> rules = SchemaManager.getAllRules(tx);
                assertEquals(2, rules.size());

                HashSet<String> expectedRuleLabels = new HashSet<String>(){
                    {
                        add("transaction-currency-is-that-of-the-country");
                        add("locates-is-transitive");
                    }
                };
                assertEquals(expectedRuleLabels, rules.stream().map(rule -> rule.label().toString()).collect(Collectors.toSet()));
            }
        }
    }

    @Test
    public void testUndefineAllRulesSuccessfullyUndefinesAllRules() {
        try (Session session = graknTestServer.session(KEYSPACE)) {
            SchemaManager.undefineAllRules(session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                List<String> ruleLabels = tx.stream(Graql.match(Graql.var("x").sub("rule")).get("x")).map(ans -> ans.get("x").asRule().label().toString()).collect(Collectors.toList());
                assertEquals(1, ruleLabels.size());
                assertEquals("rule", ruleLabels.get(0));
            }
        }
    }
}
