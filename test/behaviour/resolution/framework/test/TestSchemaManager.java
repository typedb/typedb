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

import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.TypeDB.Session;
import com.vaticle.typedb.core.TypeDB.Transaction;;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager;
import com.vaticle.typedb.core.test.rule.GraknTestServer;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.addResolutionSchema;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.connectResolutionSchema;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.test.LoadTest.loadTestStub;
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

            try (Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {

                TypeQLMatch roleplayersQuery = TypeQL.match(
                        TypeQL.var("x").plays("instance"),
                        TypeQL.var("x").plays("owner"),
                        TypeQL.var("x").plays("roleplayer")
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

            try (Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {

                TypeQLMatch roleplayersQuery = TypeQL.match(
                        TypeQL.var("x").plays("rel")
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

            try (Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {

                TypeQLMatch clauseAttributesQuery = TypeQL.match(TypeQL.var("x").sub("has-attribute-property")).get();

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
            try (Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
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
            try (Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                List<String> ruleLabels = tx.stream(TypeQL.match(TypeQL.var("x").sub("rule")).get("x")).map(ans -> ans.get("x").asRule().label().toString()).collect(Collectors.toList());
                assertEquals(1, ruleLabels.size());
                assertEquals("rule", ruleLabels.get(0));
            }
        }
    }
}
