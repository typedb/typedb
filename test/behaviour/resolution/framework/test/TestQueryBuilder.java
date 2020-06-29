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

import grakn.core.test.behaviour.resolution.framework.resolve.QueryBuilder;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static grakn.core.test.behaviour.resolution.framework.common.Utils.getStatements;
import static org.junit.Assert.assertEquals;

public class TestQueryBuilder {

    @Test
    public void testIdStatementsAreRemovedCorrectly() {
        Set<Statement> statementsWithIds = getStatements(Graql.parsePatternList(
                "$transaction has currency $currency;\n" +
                "$transaction id V86232;\n" +
                "$currency id V36912;\n" +
                "$transaction isa transaction;\n"
        ));

        Set<Statement> expectedStatements = getStatements(Graql.parsePatternList(
                "$transaction has currency $currency;\n" +
                "$transaction isa transaction;\n"
        ));
        expectedStatements.add(null);

        Set<Statement> statementsWithoutIds = statementsWithIds.stream().map(QueryBuilder::removeIdProperties).collect(Collectors.toSet());

        assertEquals(expectedStatements, statementsWithoutIds);
    }

    @Test
    public void testStatementToResolutionPropertiesForVariableAttributeOwnership() {
        Statement statement = getOnlyElement(Graql.parsePattern("$transaction has currency $currency;").statements());

        Statement expectedPropsStatement = getOnlyElement(Graql.parsePattern("$x0 (owner: $transaction) isa has-attribute-property, has currency $currency;").statements());

        Statement propsStatement = getOnlyElement(new QueryBuilder().statementToResolutionProperties(statement).values());

        assertEquals(expectedPropsStatement, propsStatement);
    }

    @Test
    public void testStatementToResolutionPropertiesForAttributeOwnership() {
        Statement statement = getOnlyElement(Graql.parsePattern("$transaction has currency \"GBP\";").statements());

        Statement expectedPropsStatement = getOnlyElement(Graql.parsePattern("$x0 (owner: $transaction) isa has-attribute-property, has currency \"GBP\";").statements());

        Statement propsStatement = getOnlyElement(new QueryBuilder().statementToResolutionProperties(statement).values());

        assertEquals(expectedPropsStatement, propsStatement);
    }

    @Test
    public void testStatementToResolutionPropertiesForRelation() {
        Statement statement = getOnlyElement(Graql.parsePattern("$locates (locates_located: $transaction, locates_location: $country);").statements());

        Set<Statement> expectedPropsStatements = getStatements(Graql.parsePatternList("" +
                "$x0 (rel: $locates, roleplayer: $transaction) isa relation-property, has role-label \"locates_located\";" +
                "$x1 (rel: $locates, roleplayer: $country) isa relation-property, has role-label \"locates_location\";"
        ));

        Set<Statement> propsStatements = new HashSet<>(new QueryBuilder().statementToResolutionProperties(statement).values());

        assertEquals(expectedPropsStatements, propsStatements);
    }

    @Test
    public void testStatementToResolutionPropertiesForIsa() {
        Statement statement = getOnlyElement(Graql.parsePattern("$transaction isa transaction;").statements());
        Statement propStatement = getOnlyElement(new QueryBuilder().statementToResolutionProperties(statement).values());
        Statement expectedPropStatement = getOnlyElement(Graql.parsePattern("$x0 (instance: $transaction) isa isa-property, has type-label \"transaction\";").statements());
        assertEquals(expectedPropStatement, propStatement);
    }

    @Test
    public void testStatementsForRuleApplication() {

        Pattern when = Graql.parsePattern("" +
                "{ $country isa country; " +
                "$transaction isa transaction;" +
                "$country has currency $currency; " +
                "$locates (locates_located: $transaction, locates_location: $country) isa locates; };"
        );

        Pattern then = Graql.parsePattern("" +
                "{ $transaction has currency $currency; };"
        );

        Pattern expected = Graql.parsePattern("" +
                "{ $x0 (owner: $country) isa has-attribute-property, has currency $currency; " +
                // TODO Should we also have an isa-property for $currency?
                "$x1 (instance: $country) isa isa-property, has type-label \"country\"; " +
                "$x2 (rel: $locates, roleplayer: $transaction) isa relation-property, has role-label \"locates_located\"; " +
                "$x3 (rel: $locates, roleplayer: $country) isa relation-property, has role-label \"locates_location\"; " +
                "$x4 (instance: $locates) isa isa-property, has type-label \"locates\"; " +
                "$x5 (instance: $transaction) isa isa-property, has type-label \"transaction\"; " + //TODO When inserted, the supertype labels should be owned too
                "$x6 (owner: $transaction) isa has-attribute-property, has currency $currency; " +
                "$_ (body: $x0, body: $x1, body: $x2, body: $x3, body: $x4, body: $x5, head: $x6) isa resolution, " +
                "has rule-label \"transaction-currency-is-that-of-the-country\"; };");


        Pattern resolution = new QueryBuilder().ruleResolutionConjunction(when, then, "transaction-currency-is-that-of-the-country");
        assertEquals(expected, resolution);
    }
}
