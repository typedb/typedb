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
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.RuleResolutionBuilder;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.variable.Variable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.test.LoadTest.loadBasicRecursionTest;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.test.LoadTest.loadComplexRecursionTest;

public class RuleResolutionBuilderIT {
    private static final String database = "RuleResolutionBuilderIT";
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve(database);
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).logsDir(logDir);
    private TypeDB typeDB;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        this.typeDB = RocksTypeDB.open(options);
        this.typeDB.databases().create(database);
    }

    @After
    public void tearDown() {
        this.typeDB.close();
    }

    @Test
    public void testVariableToResolutionPropertiesForVariableAttributeOwnership() {
        loadBasicRecursionTest(typeDB, database);
        try (Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                Variable variable = TypeQL.parsePattern("$company has name $name;").asVariable();
                Variable expectedPropsVariable = TypeQL.parsePattern("$x0 (owned: $name, owner: $company) isa has-attribute-property;").asVariable();
                Variable propsVariable = getOnlyElement(new RuleResolutionBuilder().addTrackingConstraints(variable, null).values());
                Assert.assertEquals(expectedPropsVariable, propsVariable);
            }
        }
    }

    @Test
    public void testVariableToResolutionPropertiesForAttributeOwnership() {
        loadBasicRecursionTest(typeDB, database);
        try (Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                Variable variable = TypeQL.parsePattern("$company has name \"Apple\";").asVariable();
                String regex = "^\\$x0 \\(owned: \\$\\d+, owner: \\$company\\) isa has-attribute-property;$";
                Variable propsVariable = getOnlyElement(new RuleResolutionBuilder().addTrackingConstraints(variable, null).values());
                Assert.assertTrue(propsVariable.toString().matches(regex));
            }
        }
    }

    @Test
    public void testVariableToResolutionPropertiesForRelation() {
        loadComplexRecursionTest(typeDB, database);
        try (Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                Variable variable = TypeQL.parsePattern("$locates (locates_located: $transaction, locates_location: $country);").asVariable();
                Set<Variable> expectedPropsVariables = TypeQL.parsePattern("" +
                        "$x0 (rel: $locates, roleplayer: $transaction) isa relation-property, has role-label \"locates_located\", has role-label \"role\";" +
                        "$x1 (rel: $locates, roleplayer: $country) isa relation-property, has role-label \"locates_location\", has role-label \"role\";"
                ).asConjunction().variables().collect(Collectors.toSet());
                Set<Variable> propsVariables = new HashSet<>(new RuleResolutionBuilder().addTrackingConstraints(variable, null).values());
                Assert.assertEquals(expectedPropsVariables, propsVariables);
            }
        }
    }

    @Test
    public void testVariableToResolutionPropertiesForIsa() {
        loadBasicRecursionTest(typeDB, database);
        try (Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                Variable variable = TypeQL.parsePattern("$company isa company;").asVariable();
                Variable propVariable = getOnlyElement(new RuleResolutionBuilder().addTrackingConstraints(variable, null).values());
                Variable expectedPropVariable = TypeQL.parsePattern("$x0 (instance: $company) isa isa-property, has type-label \"company\", has type-label \"entity\";").asVariable();
                Assert.assertEquals(expectedPropVariable, propVariable);
            }
        }
    }

    @Test
    public void testVariablesForRuleApplication() {
        Pattern when = TypeQL.parsePattern("" +
                "{ $country isa country; " +
                "$transaction isa transaction;" +
                "$country has currency $currency; " +
                "$locates (locates_located: $transaction, locates_location: $country) isa locates; };"
        );

        Pattern then = TypeQL.parsePattern("" +
                "{ $transaction has currency $currency; };"
        );

        // TODO: The ordering of role-labels and type-labels seems a bit arbitrary.
        Pattern expected = TypeQL.parsePattern("" +
                "{ $x0 (owned: $currency, owner: $country) isa has-attribute-property; " +
                // TODO Should we also have an isa-property for $currency?
                "$x1 (instance: $country) isa isa-property, has type-label \"location\", has type-label \"entity\", has type-label \"country\"; " +
                "$x2 (rel: $locates, roleplayer: $transaction) isa relation-property, has role-label \"role\", has role-label \"locates_located\"; " +
                "$x3 (rel: $locates, roleplayer: $country) isa relation-property, has role-label \"locates_location\", has role-label \"role\"; " +
                "$x4 (instance: $locates) isa isa-property, has type-label \"relation\", has type-label \"locates\"; " +
                "$x5 (instance: $transaction) isa isa-property, has type-label \"entity\", has type-label \"transaction\"; " + //TODO When inserted, the supertype labels should be owned too
                "$x6 (owned: $currency, owner: $transaction) isa has-attribute-property, has inferred true; " +
                "$_ (body: $x0, body: $x1, body: $x2, body: $x3, body: $x4, body: $x5, head: $x6) isa resolution, " +
                "has rule-label \"transaction-currency-is-that-of-the-country\"; };");

        loadComplexRecursionTest(typeDB, database);

        try (Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                Pattern resolution = new RuleResolutionBuilder().ruleResolutionConjunction(tx, when, then, "transaction-currency-is-that-of-the-country");
                Assert.assertEquals(expected, resolution);
            }
        }
    }
}
