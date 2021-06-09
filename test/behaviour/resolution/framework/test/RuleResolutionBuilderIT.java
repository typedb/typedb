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
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.RuleResolutionBuilder;
import com.vaticle.typedb.core.test.rule.GraknTestServer;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.statement.Statement;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.Utils.getStatements;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.test.LoadTest.loadTestStub;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RuleResolutionBuilderIT {
    @ClassRule
    public static final GraknTestServer graknTestServer = new GraknTestServer();

    @Test
    public void testStatementToResolutionPropertiesForVariableAttributeOwnership() {
        try (Session session = graknTestServer.sessionWithNewKeyspace()) {

            loadTestStub(session, "basic_recursion");

            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                Statement statement = getOnlyElement(TypeQL.parsePattern("$company has name $name;").statements());
                Statement expectedPropsStatement = getOnlyElement(TypeQL.parsePattern("$x0 (owned: $name, owner: $company) isa has-attribute-property;").statements());
                Statement propsStatement = getOnlyElement(new RuleResolutionBuilder().statementToResolutionProperties(tx, statement, null).values());
                assertEquals(expectedPropsStatement, propsStatement);
            }
        }
    }

    @Test
    public void testStatementToResolutionPropertiesForAttributeOwnership() {
        try (Session session = graknTestServer.sessionWithNewKeyspace()) {

            loadTestStub(session, "basic_recursion");

            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                Statement statement = getOnlyElement(TypeQL.parsePattern("$company has name \"Apple\";").statements());
                String regex = "^\\$x0 \\(owned: \\$\\d+, owner: \\$company\\) isa has-attribute-property;$";
                Statement propsStatement = getOnlyElement(new RuleResolutionBuilder().statementToResolutionProperties(tx, statement, null).values());
                assertTrue(propsStatement.toString().matches(regex));
            }
        }
    }

    @Test
    public void testStatementToResolutionPropertiesForRelation() {
        try (Session session = graknTestServer.sessionWithNewKeyspace()) {

            loadTestStub(session, "complex_recursion");

            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                Statement statement = getOnlyElement(TypeQL.parsePattern("$locates (locates_located: $transaction, locates_location: $country);").statements());
                Set<Statement> expectedPropsStatements = getStatements(TypeQL.parsePatternList("" +
                        "$x0 (rel: $locates, roleplayer: $transaction) isa relation-property, has role-label \"locates_located\", has role-label \"role\";" +
                        "$x1 (rel: $locates, roleplayer: $country) isa relation-property, has role-label \"locates_location\", has role-label \"role\";"
                ));
                Set<Statement> propsStatements = new HashSet<>(new RuleResolutionBuilder().statementToResolutionProperties(tx, statement, null).values());
                assertEquals(expectedPropsStatements, propsStatements);
            }
        }
    }

    @Test
    public void testStatementToResolutionPropertiesForIsa() {
        try (Session session = graknTestServer.sessionWithNewKeyspace()) {

            loadTestStub(session, "basic_recursion");

            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                Statement statement = getOnlyElement(TypeQL.parsePattern("$company isa company;").statements());
                Statement propStatement = getOnlyElement(new RuleResolutionBuilder().statementToResolutionProperties(tx, statement, null).values());
                Statement expectedPropStatement = getOnlyElement(TypeQL.parsePattern("$x0 (instance: $company) isa isa-property, has type-label \"company\", has type-label \"entity\";").statements());
                assertEquals(expectedPropStatement, propStatement);
            }
        }
    }

    @Test
    public void testStatementsForRuleApplication() {

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

        try (Session session = graknTestServer.sessionWithNewKeyspace()) {

            loadTestStub(session, "complex_recursion");

            try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                Pattern resolution = new RuleResolutionBuilder().ruleResolutionConjunction(tx, when, then, "transaction-currency-is-that-of-the-country");
                assertEquals(expected, resolution);
            }
        }
    }
}
