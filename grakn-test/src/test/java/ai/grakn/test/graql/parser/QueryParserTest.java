/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.graql.parser;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RuleType;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.AskQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.DataTypeProperty;
import ai.grakn.graql.internal.query.aggregate.AbstractAggregate;
import ai.grakn.test.AbstractMovieGraphTest;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.contains;
import static ai.grakn.graql.Graql.eq;
import static ai.grakn.graql.Graql.gt;
import static ai.grakn.graql.Graql.gte;
import static ai.grakn.graql.Graql.lt;
import static ai.grakn.graql.Graql.lte;
import static ai.grakn.graql.Graql.name;
import static ai.grakn.graql.Graql.neq;
import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.parse;
import static ai.grakn.graql.Graql.regex;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.Order.desc;
import static ai.grakn.util.Schema.MetaSchema.ENTITY;
import static ai.grakn.util.Schema.MetaSchema.RELATION;
import static ai.grakn.util.Schema.MetaSchema.ROLE;
import static ai.grakn.util.Schema.MetaSchema.RULE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class QueryParserTest extends AbstractMovieGraphTest {

    private QueryBuilder qb;
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        qb = graph.graql();
    }

    @Test
    public void testSimpleQuery() {
        assertQueriesEqual(
                qb.match(var("x").isa("movie")),
                qb.parse("match $x isa movie;")
        );
    }

    @Test
    public void testRelationQuery() {
        MatchQuery expected = qb.match(
                var("brando").value("Marl B").isa("person"),
                var().rel("actor", "brando").rel("char").rel("production-with-cast", "prod")
        ).select("char", "prod");

        MatchQuery parsed = qb.parse(
                "match\n" +
                        "$brando value \"Marl B\" isa person;\n" +
                        "(actor: $brando, $char, production-with-cast: $prod);\n" +
                        "select $char, $prod;"
        );

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testPredicateQuery1() {
        MatchQuery expected = qb.match(
                var("x").isa("movie").has("title", var("t")),
                or(
                        var("t").value(eq("Apocalypse Now")),
                        and(var("t").value(lt("Juno")), var("t").value(gt("Godfather"))),
                        var("t").value(eq("Spy"))
                ),
                var("t").value(neq("Apocalypse Now"))
        );

        MatchQuery parsed = qb.parse(
                "match\n" +
                "$x isa movie, has title $t;\n" +
                "$t value = \"Apocalypse Now\" or {$t value < 'Juno'; $t value > 'Godfather';} or $t value 'Spy';" +
                "$t value !='Apocalypse Now';\n"
        );

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testPredicateQuery2() {
        MatchQuery expected = qb.match(
                var("x").isa("movie").has("title", var("t")),
                or(
                    and(var("t").value(lte("Juno")), var("t").value(gte("Godfather")), var("t").value(neq("Heat"))),
                    var("t").value("The Muppets")
                )
        );

        MatchQuery parsed = qb.parse(
                "match $x isa movie, has title $t;" +
                "{$t value <= 'Juno'; $t value >= 'Godfather'; $t value != 'Heat';} or $t value = 'The Muppets';"
        );

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testPredicateQuery3() {
        MatchQuery expected = qb.match(
                var().rel("x").rel("y"),
                var("y").isa("person").has("name", var("n")),
                or(var("n").value(contains("ar")), var("n").value(regex("^M.*$")))
        );

        MatchQuery parsed = (MatchQuery) qb.parse(
                "match ($x, $y); $y isa person, has name $n;" +
                "$n value contains 'ar' or $n value /^M.*$/;"
        );

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testMoviesReleasedAfterOrAtTheSameTimeAsSpy() {
        MatchQuery expected = qb.match(
                var("x").has("release-date", gte(var("r"))),
                var().has("title", "Spy").has("release-date", var("r"))
        );

        MatchQuery parsed = qb.parse("match $x has release-date >= $r; has title 'Spy', has release-date $r;");

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testTypesQuery() throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

        long date = dateFormat.parse("Mon Mar 03 00:00:00 BST 1986").getTime();

        MatchQuery expected = qb.match(
                var("x")
                        .has("release-date", lt(date))
                        .has("tmdb-vote-count", 100)
                        .has("tmdb-vote-average", lte(9.0))
        );

        MatchQuery parsed = qb.parse(
                "match $x has release-date < " + date + ", has tmdb-vote-count 100 has tmdb-vote-average<=9.0;"
        );

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testLongComparatorQuery() throws ParseException {
        MatchQuery expected = qb.match(
                var("x").has("tmdb-vote-count", lte(400))
        );

        MatchQuery parsed = qb.parse("match $x isa movie, has tmdb-vote-count <= 400;");

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testModifierQuery() {
        MatchQuery expected = qb.match(
                var("y").isa("movie").has("title", var("n"))
        ).orderBy("n").limit(4).offset(2).distinct();

        MatchQuery parsed =
                qb.parse("match $y isa movie, has title $n; order by $n; limit 4; offset 2; distinct;");

        assertOrderedQueriesEqual(expected, parsed);
    }

    @Test
    public void testOntologyQuery() {
        MatchQuery expected = qb.match(var("x").playsRole("actor")).orderBy("x");
        MatchQuery parsed = qb.parse("match $x plays-role actor; order by $x asc;");
        assertOrderedQueriesEqual(expected, parsed);
    }

    @Test
    public void testOrderQuery() {
        MatchQuery expected = qb.match(var("x").isa("movie").has("release-date", var("r"))).orderBy("r", desc);
        MatchQuery parsed = qb.parse("match $x isa movie, has release-date $r; order by $r desc;");
        assertOrderedQueriesEqual(expected, parsed);
    }

    @Test
    public void testHasValueQuery() {
        MatchQuery expected = qb.match(var("x").value());
        MatchQuery parsed = qb.parse("match $x value;");
        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testVariablesEverywhereQuery() {
        MatchQuery expected = qb.match(
                var().rel(var("p"), "x").rel("y"),
                var("x").isa(var("z")),
                var("y").value("crime"),
                var("z").sub("production"),
                name("has-genre").hasRole(var("p"))
        );

        MatchQuery parsed = qb.parse(
                "match" +
                        "($p: $x, $y);" +
                        "$x isa $z;" +
                        "$y value 'crime';" +
                        "$z sub production;" +
                        "has-genre has-role $p;"
        );

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testOrQuery() {
        MatchQuery expected = qb.match(
                var("x").isa("movie"),
                or(
                        and(var("y").isa("genre").value("drama"), var().rel("x").rel("y")),
                        var("x").value("The Muppets")
                )
        );

        MatchQuery parsed = qb.parse(
                "match $x isa movie; { $y isa genre value 'drama'; ($x, $y); } or $x value 'The Muppets';"
        );

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testPositiveAskQuery() {
        assertTrue(Graql.<AskQuery>parse("match $x isa movie has title 'Godfather'; ask;").withGraph(graph).execute());
    }

    @Test
    public void testNegativeAskQuery() {
        assertFalse(qb.<AskQuery>parse("match $x isa movie type-name 'Dogfather'; ask;").execute());
    }

    @Test
    public void testConstructQuery() {
        Var var = var().isa("movie").has("title", "The Title");
        String varString = "isa movie has title \"The Title\";";
        assertFalse(qb.match(var).ask().execute());

        Graql.parse("insert " + varString).withGraph(graph).execute();
        assertTrue(qb.match(var).ask().execute());

        // TODO: Fix delete queries in titan
        assumeFalse(usingTitan());

        Graql.parse("match $x " + varString + " delete $x;").withGraph(graph).execute();
        assertFalse(qb.match(var).ask().execute());
    }

    @Test
    public void testInsertOntologyQuery() {
        qb.parse("insert " +
                "'pokemon' sub entity;" +
                "evolution sub relation;" +
                "evolves-from sub role;" +
                "type-name \"evolves-to\" sub role;" +
                "evolution has-role evolves-from, has-role evolves-to;" +
                "pokemon plays-role evolves-from plays-role evolves-to has-resource name;" +
                "name sub resource datatype string;" +
                "$x has name 'Pichu' isa pokemon;" +
                "$y has name 'Pikachu' isa pokemon;" +
                "$z has name 'Raichu' isa pokemon;" +
                "(evolves-from: $x ,evolves-to: $y) isa evolution;" +
                "(evolves-from: $y, evolves-to: $z) isa evolution;").execute();

        assertTrue(qb.match(name("pokemon").sub(ENTITY.getName())).ask().execute());
        assertTrue(qb.match(name("evolution").sub(RELATION.getName())).ask().execute());
        assertTrue(qb.match(name("evolves-from").sub(ROLE.getName())).ask().execute());
        assertTrue(qb.match(name("evolves-to").sub(ROLE.getName())).ask().execute());
        assertTrue(qb.match(name("evolution").hasRole("evolves-from").hasRole("evolves-to")).ask().execute());
        assertTrue(qb.match(name("pokemon").playsRole("evolves-from").playsRole("evolves-to")).ask().execute());

        assertTrue(qb.match(
                var("x").has("name", "Pichu").isa("pokemon"),
                var("y").has("name", "Pikachu").isa("pokemon"),
                var("z").has("name", "Raichu").isa("pokemon"),
                var().rel("evolves-from", "x").rel("evolves-to", "y").isa("evolution"),
                var().rel("evolves-from", "y").rel("evolves-to", "z").isa("evolution")
        ).ask().execute());
    }

    @Test
    public void testMatchInsertQuery() {
        Var language1 = var().isa("language").has("name", "123");
        Var language2 = var().isa("language").has("name", "456");

        qb.insert(language1, language2).execute();
        assertTrue(qb.match(language1).ask().execute());
        assertTrue(qb.match(language2).ask().execute());

        qb.parse("match $x isa language; insert $x has name \"HELLO\";").execute();
        assertTrue(qb.match(var().isa("language").has("name", "123").has("name", "HELLO")).ask().execute());
        assertTrue(qb.match(var().isa("language").has("name", "456").has("name", "HELLO")).ask().execute());

        // TODO: Fix delete queries in titan
        assumeFalse(usingTitan());

        qb.parse("match $x isa language; delete $x;").execute();
        assertFalse(qb.match(language1).ask().execute());
        assertFalse(qb.match(language2).ask().execute());
    }

    @Test
    public void testInsertIsAbstractQuery() {
        qb.parse("insert concrete-type sub entity; abstract-type is-abstract sub entity;").execute();

        assertFalse(qb.<AskQuery>parse("match concrete-type is-abstract; ask;").execute());
        assertTrue(qb.<AskQuery>parse("match abstract-type is-abstract; ask;").execute());
    }

    @Test
    public void testMatchDataTypeQuery() {
        MatchQuery expected = qb.match(var("x").datatype(ResourceType.DataType.DOUBLE));
        MatchQuery parsed = qb.parse("match $x datatype double;");

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testInsertDataTypeQuery() {
        qb.parse("insert my-type sub resource, datatype long;").execute();

        MatchQuery query = qb.match(var("x").name("my-type"));
        ResourceType.DataType datatype = query.iterator().next().get("x").asResourceType().getDataType();

        assertEquals(ResourceType.DataType.LONG, datatype);
    }

    @Test
    public void testEscapeString() {
        String unescaped = "This has \"double quotes\" and a single-quoted backslash: '\\'";
        String escaped = "This has \\\"double quotes\\\" and a single-quoted backslash: \\'\\\\\\'";

        assertFalse(qb.match(var().isa("movie").value(unescaped).has("title", unescaped)).ask().execute());

        qb.parse("insert isa movie has title '" + escaped + "';").execute();

        assertFalse(qb.match(var().isa("movie").has("title", escaped)).ask().execute());
        assertTrue(qb.match(var().isa("movie").has("title", unescaped)).ask().execute());
    }

    @Test
    public void testComments() {
        assertTrue(qb.<AskQuery>parse("match \n# there's a comment here\n$x isa###WOW HERES ANOTHER###\r\nmovie; ask;").execute());
    }

    @Test
    public void testInsertRules() {
        String ruleTypeId = "my-rule-thing";
        String lhs = "$x isa movie;";
        String rhs = "id '123' isa movie;";
        Pattern lhsPattern = and(qb.parsePatterns(lhs));
        Pattern rhsPattern = and(qb.parsePatterns(rhs));

        qb.parse("insert '" + ruleTypeId + "' sub rule; \n" +
                "isa my-rule-thing, lhs {" + lhs + "}, rhs {" + rhs + "};").execute();

        assertTrue(qb.match(name("my-rule-thing").sub(RULE.getName())).ask().execute());

        RuleType ruleType = graph.getRuleType(ruleTypeId);
        boolean found = false;
        for (ai.grakn.concept.Rule rule : ruleType.instances()) {
            if(lhsPattern.equals(rule.getLHS()) && rhsPattern.equals(rule.getRHS())){
                found = true;
                break;
            }
        }
        assertTrue("Unable to find rule with lhs [" + lhsPattern + "] and rhs [" + rhsPattern + "]", found);
    }

    @Test
    public void testQueryParserWithoutGraph() {
        String queryString = "match $x isa movie; select $x;";
        MatchQuery query = parse("match $x isa movie; select $x;");
        assertEquals(queryString, query.toString());
        assertTrue(query.withGraph(graph).stream().findAny().isPresent());
    }

    @Test
    public void testParseBoolean() {
        assertEquals("insert has flag true;", qb.parse("insert has flag true;").toString());
    }

    @Test
    public void testParseAggregate() {
        //noinspection unchecked
        AggregateQuery<Map<String, Object>> query = (AggregateQuery<Map<String, Object>>)
                qb.parse("match $x isa movie; aggregate (count as c, group $x as g);");

        Map<String, Object> result = query.execute();

        assertTrue(result.get("c") instanceof Long);
        assertTrue(result.get("g") instanceof Map);
    }

    @Test
    public void testParseAggregateToString() {
        String query = "match $x isa movie; aggregate group $x (count as c);";
        assertEquals(query, ((AggregateQuery<?>) parse(query)).withGraph(graph).toString());
    }

    @Test
    public void testCustomAggregate() {
        QueryBuilder qb = graph.graql();

        qb.registerAggregate(
                "get-any", args -> new AbstractAggregate<Map<String, Concept>, Concept>() {
                    @Override
                    public Concept apply(Stream<? extends Map<String, Concept>> stream) {
                        //noinspection OptionalGetWithoutIsPresent,SuspiciousMethodCalls
                        return stream.findAny().get().get(args.get(0));
                    }
                }
        );

        //noinspection unchecked
        AggregateQuery<Concept> query =
                (AggregateQuery<Concept>) qb.parse("match $x isa movie; aggregate get-any $x;");

        Concept result = query.execute();

        assertEquals("movie", result.type().getName());
    }

    @Test
    public void testParseComputeCount() {
        assertParseEquivalence("compute count;");
    }

    @Test
    public void testParseComputeCountWithSubgraph() {
        assertParseEquivalence("compute count in movie, person;");
    }

    @Test
    public void testParseComputeCluster() {
        assertParseEquivalence("compute cluster in movie, person; members; persist;");
    }

    @Test
    public void testParseComputeDegree() {
        assertParseEquivalence("compute degrees in movie; persist;");
    }

    @Test
    public void testParseComputeMax() {
        assertParseEquivalence("compute max of person in movie;");
    }

    @Test
    public void testParseComputeMean() {
        assertParseEquivalence("compute mean of person in movie;");
    }

    @Test
    public void testParseComputeMedian() {
        assertParseEquivalence("compute median of person in movie;");
    }

    @Test
    public void testParseComputeMin() {
        assertParseEquivalence("compute min of movie in person;");
    }

    @Test
    public void testParseComputePath() {
        assertParseEquivalence("compute path from \"1\" to \"2\" in person;");
    }

    @Test
    public void testParseComputeStd() {
        assertParseEquivalence("compute std of movie;");
    }

    @Test
    public void testParseComputeSum() {
        assertParseEquivalence("compute sum of movie in person;");
    }

    @Test
    public void testBadSyntaxThrowsIllegalArgumentException() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(allOf(
                containsString("syntax error"), containsString("line 1"),
                containsString("\nmatch $x isa "),
                containsString("\n             ^")
        ));
        qb.parse("match $x isa ");
    }

    @Test
    public void testSyntaxErrorPointer() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(allOf(
                containsString("\nmatch $x is"),
                containsString("\n         ^")
        ));
        qb.parse("match $x is");
    }

    @Test
    public void testHasVariable() {
        MatchQuery query = qb.parse("match has title 'Godfather' has tmdb-vote-count $x;");

        //noinspection OptionalGetWithoutIsPresent
        assertEquals(1000L, query.get("x").findFirst().get().asResource().getValue());
    }

    @Test
    public void testRegexResourceType() {
        MatchQuery query = qb.parse("match $x regex /(fe)?male/;");
        assertEquals(1, query.stream().count());
        assertEquals("gender", query.get("x").findFirst().get().asType().getName());
    }

    @Test
    public void testGraqlParseQuery() {
        assertTrue(parse("match $x isa movie;") instanceof MatchQuery);
    }

    @Test
    public void testParseBooleanType() {
        MatchQuery query = parse("match $x datatype boolean;");

        VarAdmin var = query.admin().getPattern().getVars().iterator().next();

        //noinspection OptionalGetWithoutIsPresent
        DataTypeProperty property = var.getProperty(DataTypeProperty.class).get();

        Assert.assertEquals(ResourceType.DataType.BOOLEAN, property.getDatatype());
    }

    @Test
    public void testParseHasScope() {
        assertEquals("match $x has-scope $y;", parse("match $x has-scope $y;").toString());
    }

    @Test
    public void testParseKey() {
        assertEquals("match $x has-key name;", parse("match $x has-key name;").toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMultipleQueriesThrowsIllegalArgumentException() {
        qb.<InsertQuery>parse("insert $x isa movie; insert $y isa movie").execute();
    }

    @Test
    public void testMissingColon() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(containsString("':'"));
        qb.parse("match (actor $x, $y) isa has-cast;");
    }

    @Test
    public void testMissingComma() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(containsString("','"));
        qb.parse("match ($x $y) isa has-cast;");
    }

    @Test
    public void testAdditionalSemicolon() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("id"), containsString("plays-role product-type")));
        qb.parse(
                "insert " +
                "tag-group sub role; product-type sub role;" +
                "category sub entity, plays-role tag-group; plays-role product-type;"
        ).execute();
    }

    @Test
    public void testLimitMistake() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(containsString("limit1"));
        qb.parse("match ($x, $y); limit1;");
    }

    private void assertOrderedQueriesEqual(MatchQuery query, MatchQuery parsedQuery) {
        assertEquals(
                Lists.newArrayList(query).toString(),
                Lists.newArrayList(parsedQuery).toString()
        );
    }

    public static void assertQueriesEqual(MatchQuery query, MatchQuery parsedQuery) {
        assertEquals(Sets.newHashSet(query), Sets.newHashSet(parsedQuery));
    }

    private static void assertParseEquivalence(String query) {
        assertEquals(query, Graql.parse(query).toString());
    }
}