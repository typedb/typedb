package grakn.core.test.behaviour.resolution.test;

import grakn.core.test.behaviour.resolution.resolve.QueryBuilder;
import graql.lang.Graql;
import graql.lang.statement.Statement;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static grakn.core.test.behaviour.resolution.common.Utils.getStatements;
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

        Set<Statement> statementsWithoutIds = QueryBuilder.removeIdProperties(statementsWithIds);

        assertEquals(expectedStatements, statementsWithoutIds);
    }

    @Test
    public void testStatementToPropertiesForVariableAttributeOwnership() {
        Statement statement = getOnlyElement(Graql.parsePattern("$transaction has currency $currency;").statements());

        Statement expectedPropsStatement = getOnlyElement(Graql.parsePattern("$x0 (owner: $transaction) isa has-attribute-property, has currency $currency;").statements());

        Statement propsStatement = getOnlyElement(new QueryBuilder().statementToResolutionProperties(statement).values());

        assertEquals(expectedPropsStatement, propsStatement);
    }

    @Test
    public void testStatementToPropertiesForAttributeOwnership() {
        Statement statement = getOnlyElement(Graql.parsePattern("$transaction has currency \"GBP\";").statements());

        Statement expectedPropsStatement = getOnlyElement(Graql.parsePattern("$x0 (owner: $transaction) isa has-attribute-property, has currency \"GBP\";").statements());

        Statement propsStatement = getOnlyElement(new QueryBuilder().statementToResolutionProperties(statement).values());

        assertEquals(expectedPropsStatement, propsStatement);
    }

    @Test
    public void testStatementToPropertiesForRelation() {
        Statement statement = getOnlyElement(Graql.parsePattern("$locates (locates_located: $transaction, locates_location: $country);").statements());

        Set<Statement> expectedPropsStatements = getStatements(Graql.parsePatternList("" +
                "$x0 (rel: $locates, roleplayer: $transaction) isa relation-property, has role-label \"locates_located\";" +
                "$x1 (rel: $locates, roleplayer: $country) isa relation-property, has role-label \"locates_location\";"
        ));

        Set<Statement> propsStatements = new HashSet<>(new QueryBuilder().statementToResolutionProperties(statement).values());

        assertEquals(expectedPropsStatements, propsStatements);
    }

    @Test
    public void testStatementToPropertiesForIsa() {
        Statement statement = getOnlyElement(Graql.parsePattern("$transaction isa transaction;").statements());
        Statement propStatement = getOnlyElement(new QueryBuilder().statementToResolutionProperties(statement).values());
        Statement expectedPropStatement = getOnlyElement(Graql.parsePattern("$x0 (instance: $transaction) isa isa-property, has type-label \"transaction\";").statements());
        assertEquals(expectedPropStatement, propStatement);
    }

    @Test
    public void testStatementsForRuleApplication() {

        Set<Statement> whenStatements = getStatements(Graql.parsePatternList("" +
                "$country isa country; " +
                "$transaction isa transaction;" +
                "$country has currency $currency; " +
                "$locates (locates_located: $transaction, locates_location: $country) isa locates; "
        ));

        Set<Statement> thenStatements = getStatements(Graql.parsePatternList("" +
                "$transaction has currency $currency; "
        ));

        Set<Statement> expectedStatements = getStatements(Graql.parsePatternList("" +
                "$x0 (instance: $country) isa isa-property, has type-label \"country\";" +
                "$x1 (instance: $transaction) isa isa-property, has type-label \"transaction\";" + //TODO When inserted, the supertype labels should be owned too
                // TODO Should we also have an isa-property for $currency?
                "$x2 (owner: $country) isa has-attribute-property, has currency $currency;" +

                "$x3 (rel: $locates, roleplayer: $transaction) isa relation-property, has role-label \"locates_located\";" + //TODO When inserted, the role supertype labels should be owned too, solved if this is done as a role rather than attribute ownership
                "$x4 (rel: $locates, roleplayer: $country) isa relation-property, has role-label \"locates_location\";" +
                "$x5 (instance: $locates) isa isa-property, has type-label \"locates\";" +

                "$x6 (owner: $transaction) isa has-attribute-property, has currency $currency;" +

                "$_ (\n" +
                "    body: $x0,\n" +
                "    body: $x1,\n" +
                "    body: $x2,\n" +
                "    body: $x3,\n" +
                "    body: $x4,\n" +
                "    body: $x5,\n" +
                "    head: $x6\n" +
                ") isa resolution, \n" +
                "has rule-label \"transaction-currency-is-that-of-the-country\";"));  //TODO can be split into conjunction

        Set<Statement> resolutionStatements;

        resolutionStatements = new QueryBuilder().inferenceStatements(whenStatements, thenStatements, "transaction-currency-is-that-of-the-country");
        assertEquals(expectedStatements, resolutionStatements);
    }
}
