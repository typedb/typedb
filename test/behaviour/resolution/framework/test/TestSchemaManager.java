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

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.TypeDB.Session;
import com.vaticle.typedb.core.TypeDB.Transaction;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.addResolutionSchema;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.connectResolutionSchema;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.test.LoadTest.loadComplexRecursionTest;
import static org.junit.Assert.assertEquals;

public class TestSchemaManager {

    private static final String database = "TestSchemaManager";
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve(database);
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).logsDir(logDir);
    private TypeDB typeDB;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        this.typeDB = RocksTypeDB.open(options);
        this.typeDB.databases().create(database);
        loadComplexRecursionTest(typeDB, database);
    }

    @After
    public void tearDown() {
        this.typeDB.close();
    }

    @Test
    public void testResolutionSchemaRolesPlayedAreCorrect() {
        try (Session session = typeDB.session(database, Arguments.Session.Type.SCHEMA)) {
            addResolutionSchema(session);
            connectResolutionSchema(session);
            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                TypeQLMatch roleplayersQuery = TypeQL.match(
                        TypeQL.var("x").plays("isa-property", "instance"),
                        TypeQL.var("x").plays("has-attribute-property", "owner"),
                        TypeQL.var("x").plays("relation-property", "roleplayer")
                );
                Set<String> roleplayers = tx.query().match(roleplayersQuery).map(r -> r.get("x").asType().getLabel().toString()).toSet();
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
        try (Session session = typeDB.session(database, Arguments.Session.Type.SCHEMA)) {
            addResolutionSchema(session);
            connectResolutionSchema(session);
            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                TypeQLMatch roleplayersQuery = TypeQL.match(
                        TypeQL.var("x").plays("relation-property", "rel")
                );
                Set<String> roleplayers = tx.query().match(roleplayersQuery).map(r -> r.get("x").asType().getLabel().toString()).toSet();
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

        try (Session session = typeDB.session(database, Arguments.Session.Type.SCHEMA)) {
            addResolutionSchema(session);
            connectResolutionSchema(session);
            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                TypeQLMatch clauseAttributesQuery = TypeQL.match(TypeQL.var("x").sub("has-attribute-property"));
                Set<String> roles = tx.query().match(clauseAttributesQuery).next().get("x").asRelationType()
                        .getRelates().map(a -> a.getLabel().toString()).toSet();
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
        try (Session session = typeDB.session(database, Arguments.Session.Type.SCHEMA)) {
            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                Set<Rule> rules = SchemaManager.getAllRules(tx);
                assertEquals(2, rules.size());
                HashSet<String> expectedRuleLabels = new HashSet<String>(){
                    {
                        add("transaction-currency-is-that-of-the-country");
                        add("locates-is-transitive");
                    }
                };
                assertEquals(expectedRuleLabels, rules.stream().map(Rule::getLabel).collect(Collectors.toSet()));
            }
        }
    }

    @Ignore // TODO: Ignored because we are unable to query for all of the rules present
    @Test
    public void testUndefineAllRulesSuccessfullyUndefinesAllRules() {
        try (Session session = typeDB.session(database, Arguments.Session.Type.SCHEMA)) {
            SchemaManager.undefineAllRules(session);
            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                // List<String> ruleLabels = tx.query().match(
                //         TypeQL.match(TypeQL.var("x").sub("rule")).get("x"))
                //         .map(ans -> ans.get("x").asRule().label().toString()).collect(Collectors.toList());
                // assertEquals(1, ruleLabels.size());
                // assertEquals("rule", ruleLabels.get(0));
            }
        }
    }
}
