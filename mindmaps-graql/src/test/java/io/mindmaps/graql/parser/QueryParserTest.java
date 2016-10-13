/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.parser;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.graql.*;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.pattern.property.DataTypeProperty;
import io.mindmaps.graql.internal.query.aggregate.AbstractAggregate;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static io.mindmaps.graql.Graql.all;
import static io.mindmaps.graql.Graql.and;
import static io.mindmaps.graql.Graql.any;
import static io.mindmaps.graql.Graql.contains;
import static io.mindmaps.graql.Graql.eq;
import static io.mindmaps.graql.Graql.gt;
import static io.mindmaps.graql.Graql.gte;
import static io.mindmaps.graql.Graql.id;
import static io.mindmaps.graql.Graql.lt;
import static io.mindmaps.graql.Graql.lte;
import static io.mindmaps.graql.Graql.neq;
import static io.mindmaps.graql.Graql.or;
import static io.mindmaps.graql.Graql.parse;
import static io.mindmaps.graql.Graql.parseAggregate;
import static io.mindmaps.graql.Graql.parseCompute;
import static io.mindmaps.graql.Graql.parseDelete;
import static io.mindmaps.graql.Graql.regex;
import static io.mindmaps.graql.Graql.var;
import static io.mindmaps.graql.Graql.withGraph;
import static io.mindmaps.util.Schema.MetaType.ENTITY_TYPE;
import static io.mindmaps.util.Schema.MetaType.RELATION_TYPE;
import static io.mindmaps.util.Schema.MetaType.ROLE_TYPE;
import static io.mindmaps.util.Schema.MetaType.RULE_TYPE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryParserTest {

    private static MindmapsGraph mindmapsGraph;
    private QueryBuilder qb;
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void setUpClass() {
        mindmapsGraph = Mindmaps.factory(Mindmaps.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
    }

    @Before
    public void setUp() {
        qb = withGraph(mindmapsGraph);
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
                var("x").isa("movie")
                        .value(any(eq("Apocalypse Now"), lt("Juno").and(gt("Godfather")), eq("Spy")).and(neq("Apocalypse Now")))
        );

        MatchQuery parsed = qb.parse(
                "match\n" +
                        "$x isa movie\n" +
                        "\tvalue (= \"Apocalypse Now\" or < 'Juno' and > 'Godfather' or 'Spy') and !='Apocalypse Now';\n"
        );

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testPredicateQuery2() {
        MatchQuery expected = qb.match(
                var("x").isa("movie").value(all(lte("Juno"), gte("Godfather"), neq("Heat")).or(eq("The Muppets")))
        );

        MatchQuery parsed = qb.parse(
                "match $x isa movie, value (<= 'Juno' and >= 'Godfather' and != 'Heat') or = 'The Muppets';"
        );

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testPredicateQuery3() {
        MatchQuery expected = qb.match(
                var().rel("x").rel("y"),
                var("y").isa("person").value(contains("ar").or(regex("^M.*$")))
        );

        MatchQuery parsed = (MatchQuery) qb.parse(
                "match ($x, $y); $y isa person value contains 'ar' or /^M.*$/;"
        );

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
                var().rel("x").rel("y"),
                var("y").isa("movie")
        ).limit(4).offset(2).distinct().orderBy("y");

        MatchQuery parsed =
                qb.parse("match ($x, $y); $y isa movie; limit 4; offset 2; distinct; order by $y;");

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
        MatchQuery expected = qb.match(var("x").isa("movie").has("release-date", var("r"))).orderBy("r", false);
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
    public void testHasTmdbVoteCountQuery() {
        MatchQuery expected = qb.match(var("x").has("tmdb-vote-count"));
        MatchQuery parsed = qb.parse("match $x has tmdb-vote-count;");
        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testVariablesEverywhereQuery() {
        MatchQuery expected = qb.match(
                var().rel(var("p"), "x").rel("y"),
                var("x").isa(var("z")),
                var("y").value("crime"),
                var("z").ako("production"),
                id("has-genre").hasRole(var("p"))
        );

        MatchQuery parsed = qb.parse(
                "match" +
                        "($p: $x, $y);" +
                        "$x isa $z;" +
                        "$y value 'crime';" +
                        "$z ako production;" +
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
        assertTrue(Graql.<AskQuery>parse("match $x isa movie id 'Godfather'; ask;").withGraph(mindmapsGraph).execute());
    }

    @Test
    public void testNegativeAskQuery() {
        assertFalse(qb.<AskQuery>parse("match $x isa movie id 'Dogfather'; ask;").execute());
    }

    @Test
    public void testConstructQuery() {
        Var var = var().id("123").isa("movie").has("title", "The Title");
        String varString = "id \"123\", isa movie has title \"The Title\";";
        assertFalse(qb.match(var).ask().execute());

        Graql.<InsertQuery>parse("insert " + varString).withGraph(mindmapsGraph).execute();
        assertTrue(qb.match(var).ask().execute());

        parseDelete("match $x " + varString + " delete $x;").withGraph(mindmapsGraph).execute();
        assertFalse(qb.match(var).ask().execute());
    }

    @Test
    public void testInsertOntologyQuery() {
        qb.<InsertQuery>parse("insert " +
                "'pokemon' isa entity-type;" +
                "evolution isa relation-type;" +
                "evolves-from isa role-type;" +
                "id \"evolves-to\" isa role-type;" +
                "evolution has-role evolves-from, has-role evolves-to;" +
                "pokemon plays-role evolves-from plays-role evolves-to;" +
                "$x id 'Pichu' isa pokemon;" +
                "$y id 'Pikachu' isa pokemon;" +
                "$z id 'Raichu' isa pokemon;" +
                "(evolves-from: $x ,evolves-to: $y) isa evolution;" +
                "(evolves-from: $y, evolves-to: $z) isa evolution;").execute();

        assertTrue(qb.match(id("pokemon").isa(ENTITY_TYPE.getId())).ask().execute());
        assertTrue(qb.match(id("evolution").isa(RELATION_TYPE.getId())).ask().execute());
        assertTrue(qb.match(id("evolves-from").isa(ROLE_TYPE.getId())).ask().execute());
        assertTrue(qb.match(id("evolves-to").isa(ROLE_TYPE.getId())).ask().execute());
        assertTrue(qb.match(id("evolution").hasRole("evolves-from").hasRole("evolves-to")).ask().execute());
        assertTrue(qb.match(id("pokemon").playsRole("evolves-from").playsRole("evolves-to")).ask().execute());

        assertTrue(qb.match(
                var("x").id("Pichu").isa("pokemon"),
                var("y").id("Pikachu").isa("pokemon"),
                var("z").id("Raichu").isa("pokemon"),
                var().rel("evolves-from", "x").rel("evolves-to", "y").isa("evolution"),
                var().rel("evolves-from", "y").rel("evolves-to", "z").isa("evolution")
        ).ask().execute());
    }

    @Test
    public void testMatchInsertQuery() {
        Var language1 = var().isa("language").id("123");
        Var language2 = var().isa("language").id("456");

        qb.insert(language1, language2).execute();
        assertTrue(qb.match(language1).ask().execute());
        assertTrue(qb.match(language2).ask().execute());

        qb.<InsertQuery>parse("match $x isa language; insert $x has name \"HELLO\";").execute();
        assertTrue(qb.match(var().isa("language").id("123").has("name", "HELLO")).ask().execute());
        assertTrue(qb.match(var().isa("language").id("456").has("name", "HELLO")).ask().execute());

        qb.parseDelete("match $x isa language; delete $x;").execute();
        assertFalse(qb.match(language1).ask().execute());
        assertFalse(qb.match(language2).ask().execute());
    }

    @Test
    public void testInsertIsAbstractQuery() {
        qb.<InsertQuery>parse("insert concrete-type isa entity-type; abstract-type is-abstract isa entity-type;").execute();

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
        qb.<InsertQuery>parse("insert my-type isa resource-type, datatype long;").execute();

        MatchQuery query = qb.match(var("x").id("my-type"));
        ResourceType.DataType datatype = query.iterator().next().get("x").asResourceType().getDataType();

        assertEquals(ResourceType.DataType.LONG, datatype);
    }

    @Test
    public void testEscapeString() {
        String unescaped = "This has \"double quotes\" and a single-quoted backslash: '\\'";
        String escaped = "This has \\\"double quotes\\\" and a single-quoted backslash: \\'\\\\\\'";

        assertFalse(qb.match(var().isa("movie").value(unescaped).has("title", unescaped)).ask().execute());

        qb.<InsertQuery>parse("insert isa movie has title '" + escaped + "';").execute();

        assertFalse(qb.match(var().isa("movie").has("title", escaped)).ask().execute());
        assertTrue(qb.match(var().isa("movie").has("title", unescaped)).ask().execute());
    }

    @Test
    public void testComments() {
        assertTrue(qb.<AskQuery>parse("match \n# there's a comment here\n$x isa###WOW HERES ANOTHER###\r\nmovie; ask;").execute());
    }

    @Test
    public void testInsertRules() {
        String lhs = "match $x isa movie;";
        String rhs = "insert id '123' isa movie;";

        qb.<InsertQuery>parse("insert id 'my-rule-thing' isa rule-type; \n" +
                "id 'rulerule' isa my-rule-thing, lhs {" + lhs + "}, rhs {" + rhs + "};").execute();

        assertTrue(qb.match(var().id("my-rule-thing").isa(RULE_TYPE.getId())).ask().execute());
        assertTrue(qb.match(var().id("rulerule").isa("my-rule-thing").lhs(lhs).rhs(rhs)).ask().execute());
    }

    @Test
    public void testQueryParserWithoutGraph() {
        String queryString = "match $x isa movie; select $x;";
        MatchQuery query = parse("match $x isa movie; select $x;");
        assertEquals(queryString, query.toString());
        assertTrue(query.withGraph(mindmapsGraph).stream().findAny().isPresent());
    }

    @Test
    public void testParseBoolean() {
        assertEquals("insert has flag true;", qb.<InsertQuery>parse("insert has flag true;").toString());
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
        assertEquals(query, parseAggregate(query).withGraph(mindmapsGraph).toString());
    }

    @Test
    public void testCustomAggregate() {
        QueryBuilder qb = Graql.withGraph(mindmapsGraph);

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

        assertEquals("movie", result.type().getId());
    }

    @Test
    public void testParseCompute() {
        assertEquals("compute count;", parseCompute("compute count;").toString());
    }

    @Test
    public void testParseComputeWithSubgraph() {
        assertEquals(
                "compute count in movie, person;",
                parseCompute("compute count in movie, person;").toString()
        );
    }

    @Test
    public void testBadSyntaxThrowsIllegalArgumentException() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(allOf(
                containsString("syntax error"), containsString("line 1"),
                containsString("\nmatch $x isa "),
                containsString("\n             ^"), containsString("EOF")
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
        MatchQuery query = qb.parse("match Godfather has tmdb-vote-count $x;");

        //noinspection OptionalGetWithoutIsPresent
        assertEquals(1000L, query.get("x").findFirst().get().asResource().getValue());
    }

    @Test
    public void testRegexResourceType() {
        MatchQuery query = qb.parse("match $x regex /(fe)?male/;");
        assertEquals(1, query.stream().count());
        assertEquals("gender", query.get("x").findFirst().get().getId());
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

        assertEquals(ResourceType.DataType.BOOLEAN, property.getDatatype());
    }

    @Test
    public void testParseHasScope() {
        assertEquals("match $x has-scope $y;", parse("match $x has-scope $y;").toString());
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
        qb.parseInsert(
                "insert " +
                "tag-group isa role-type; product-type isa role-type;" +
                "category isa entity-type, plays-role tag-group; plays-role product-type;"
        ).execute();
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
}