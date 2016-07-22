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

package io.mindmaps.graql.api.parser;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.implementation.Data;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.graql.api.query.QueryBuilder;
import io.mindmaps.graql.api.query.Var;
import io.mindmaps.graql.internal.parser.MatchQueryPrinter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import static io.mindmaps.core.implementation.DataType.ConceptMeta.*;
import static io.mindmaps.graql.api.query.QueryBuilder.*;
import static io.mindmaps.graql.api.query.ValuePredicate.*;
import static org.junit.Assert.*;

public class QueryParserTest {

    private static MindmapsTransaction transaction;
    private QueryParser qp;
    private QueryBuilder qb;

    @BeforeClass
    public static void setUpClass() {
        MindmapsGraph mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
        transaction = mindmapsGraph.newTransaction();
    }

    @Before
    public void setUp() {
        qp = QueryParser.create(transaction);
        qb = QueryBuilder.build(transaction);
    }

    @Test
    public void testSimpleQuery() {
        assertQueriesEqual(
                qb.match(var("x").isa("movie")),
                qp.parseMatchQuery("match $x isa movie")
        );
    }

    @Test
    public void testRelationQuery() {
        MatchQuery expected = qb.match(
                var("brando").value("Marl B").isa("person"),
                var().rel("actor", "brando").rel("char").rel("production-with-cast", "prod")
        ).select("char", "prod");

        MatchQueryPrinter parsed = qp.parseMatchQuery(
                "match\n" +
                        "$brando value \"Marl B\" isa person;\n" +
                        "(actor $brando, $char, production-with-cast $prod)\n" +
                        "select $char, $prod"
        );

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testPredicateQuery1() {
        MatchQuery expected = qb.match(
                var("x").isa("movie")
                        .value(any(eq("Apocalypse Now"), lt("Juno").and(gt("Godfather")), eq("Spy")).and(neq("Apocalypse Now")))
        );

        MatchQueryPrinter parsed = qp.parseMatchQuery(
                "match\n" +
                        "$x isa movie\n" +
                        "\tvalue (= \"Apocalypse Now\" or < 'Juno' and > 'Godfather' or 'Spy') and !='Apocalypse Now'\n"
        );

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testPredicateQuery2() {
        MatchQuery expected = qb.match(
                var("x").isa("movie").value(all(lte("Juno"), gte("Godfather"), neq("Heat")).or(eq("The Muppets")))
        );

        MatchQueryPrinter parsed = qp.parseMatchQuery(
                "match $x isa movie, value (<= 'Juno' and >= 'Godfather' and != 'Heat') or = 'The Muppets'"
        );

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testPredicateQuery3() {
        MatchQuery expected = qb.match(
                var().rel("x").rel("y"),
                var("y").isa("person").value(contains("ar").or(regex("^M.*$")))
        );

        MatchQueryPrinter parsed = (MatchQueryPrinter) qp.parseQuery(
                "match ($x, $y); $y isa person value contains 'ar' or /^M.*$/"
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

        MatchQueryPrinter parsed = qp.parseMatchQuery(
                "match $x has release-date < " + date + ", has tmdb-vote-count 100 has tmdb-vote-average<=9.0"
        );

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testLongComparatorQuery() throws ParseException {
        MatchQuery expected = qb.match(
                var("x").has("tmdb-vote-count", lte(400))
        );

        MatchQueryPrinter parsed = qp.parseMatchQuery("match $x isa movie, has tmdb-vote-count <= 400");

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testModifierQuery() {
        MatchQuery expected = qb.match(
                var().rel("x").rel("y"),
                var("y").isa("movie")
        ).limit(4).offset(2).distinct().orderBy("y");

        MatchQueryPrinter parsed =
                qp.parseMatchQuery("match ($x, $y); $y isa movie; limit 4 offset 2, distinct order by $y");

        assertOrderedQueriesEqual(expected, parsed);
    }

    @Test
    public void testOntologyQuery() {
        MatchQuery expected = qb.match(var("x").playsRole("actor")).orderBy("x");
        MatchQueryPrinter parsed = qp.parseMatchQuery("match $x plays-role actor, order by $x asc");
        assertOrderedQueriesEqual(expected, parsed);
    }

    @Test
    public void testGetterQuery() {
        MatchQuery expected = qb.match(var("x").isa("movie"), var().rel("x").rel("y")).select("x", "y");

        MatchQueryPrinter parsed = qp.parseMatchQuery(
                "match $x isa movie; ($x, $y) select $x(id, has release-date), $y(value isa)"
        );

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testOrderQuery() {
        MatchQuery expected = qb.match(var("x").isa("movie")).orderBy("x", "release-date", false);
        MatchQueryPrinter parsed = qp.parseMatchQuery("match $x isa movie order by $x(has release-date) desc");
        assertOrderedQueriesEqual(expected, parsed);
    }

    @Test
    public void testHasValueQuery() {
        MatchQuery expected = qb.match(var("x").value());
        MatchQueryPrinter parsed = qp.parseMatchQuery("match $x value");
        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testHasTmdbVoteCountQuery() {
        MatchQuery expected = qb.match(var("x").has("tmdb-vote-count"));
        MatchQueryPrinter parsed = qp.parseMatchQuery("match $x has tmdb-vote-count");
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

        MatchQueryPrinter parsed = qp.parseMatchQuery(
                "match" +
                        "($p $x, $y);" +
                        "$x isa $z;" +
                        "$y value 'crime';" +
                        "$z ako production;" +
                        "has-genre has-role $p"
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

        MatchQueryPrinter parsed = qp.parseMatchQuery(
                "match $x isa movie; { $y isa genre value 'drama'; ($x, $y) } or $x value 'The Muppets'"
        );

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testPositiveAskQuery() {
        assertTrue(qp.parseAskQuery("match $x isa movie value 'Godfather' ask").execute());
    }

    @Test
    public void testNegativeAskQuery() {
        assertFalse(qp.parseAskQuery("match $x isa movie value 'Dogfather' ask").execute());
    }

    @Test
    public void testConstructQuery() {
        Var var = var().id("123").value("abc").isa("movie").has("title", "The Title");
        String varString = "id \"123\", value \"abc\" isa movie has title \"The Title\"";
        assertFalse(qb.match(var).ask().execute());

        qp.parseInsertQuery("insert " + varString).execute();
        assertTrue(qb.match(var).ask().execute());

        qp.parseDeleteQuery("match $x " + varString + " delete $x").execute();
        assertFalse(qb.match(var).ask().execute());
    }

    @Test
    public void testInsertOntologyQuery() {
        qp.parseInsertQuery(
                "insert " +
                "'pokemon' isa entity-type;" +
                "evolution isa relation-type;" +
                "evolves-from isa role-type;" +
                "id \"evolves-to\" isa role-type;" +
                "evolution has-role evolves-from, has-role evolves-to;" +
                "pokemon plays-role evolves-from plays-role evolves-to;" +
                "$x id 'Pichu' isa pokemon;" +
                "$y id 'Pikachu' isa pokemon;" +
                "$z id 'Raichu' isa pokemon;" +
                "(evolves-from $x ,evolves-to $y) isa evolution;" +
                "(evolves-from $y, evolves-to $z) isa evolution;"
        ).execute();

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

        qp.parseInsertQuery("match $x isa language insert $x value \"HELLO\"").execute();
        assertTrue(qb.match(var().isa("language").id("123").value("HELLO")).ask().execute());
        assertTrue(qb.match(var().isa("language").id("456").value("HELLO")).ask().execute());

        qp.parseDeleteQuery("match $x isa language delete $x").execute();
        assertFalse(qb.match(language1).ask().execute());
        assertFalse(qb.match(language2).ask().execute());
    }

    @Test
    public void testInsertIsAbstractQuery() {
        qp.parseInsertQuery(
                "insert concrete-type isa entity-type; abstract-type is-abstract isa entity-type"
        ).execute();

        assertFalse(qp.parseAskQuery("match concrete-type is-abstract ask").execute());
        assertTrue(qp.parseAskQuery("match abstract-type is-abstract ask").execute());
    }

    @Test
    public void testMatchDataTypeQuery() {
        MatchQuery expected = qb.match(var("x").datatype(Data.DOUBLE));
        MatchQueryPrinter parsed = qp.parseMatchQuery("match $x datatype double");

        assertQueriesEqual(expected, parsed);
    }

    @Test
    public void testInsertDataTypeQuery() {
        qp.parseInsertQuery("insert my-type isa resource-type, datatype long").execute();

        MatchQuery query = qb.match(var("x").id("my-type"));
        Data datatype = query.iterator().next().get("x").asResourceType().getDataType();

        assertEquals(Data.LONG, datatype);
    }

    @Test
    public void testEscapeString() {
        String unescaped = "This has \"double quotes\" and a single-quoted backslash: '\\'";
        String escaped = "This has \\\"double quotes\\\" and a single-quoted backslash: \\'\\\\\\'";

        assertFalse(qb.match(var().isa("movie").value(unescaped).has("title", unescaped)).ask().execute());

        qp.parseInsertQuery("insert isa movie value \"" + escaped + "\", has title '" + escaped + "'").execute();

        assertFalse(qb.match(var().isa("movie").value(escaped).has("title", escaped)).ask().execute());
        assertTrue(qb.match(var().isa("movie").value(unescaped).has("title", unescaped)).ask().execute());
    }

    @Test
    public void testComments() {
        assertTrue(qp.parseAskQuery(
                "match \n# there's a comment here\n$x isa###WOW HERES ANOTHER###\r\nmovie ask"
        ).execute());
    }

    @Test
    public void testInsertRules() {
        String lhs = "match $x isa movie";
        String rhs = "insert id '123' isa movie";

        qp.parseInsertQuery(
                "insert id 'my-rule-thing' isa rule-type; \n" +
                "id 'rulerule' isa my-rule-thing, lhs {" + lhs + "}, rhs {" + rhs + "}"
        ).execute();

        assertTrue(qb.match(var().id("my-rule-thing").isa(RULE_TYPE.getId())).ask().execute());
        assertTrue(qb.match(var().id("rulerule").isa("my-rule-thing").lhs(lhs).rhs(rhs)).ask().execute());
    }

    @Test
    public void testQueryParserWithoutGraph() {
        QueryParser queryParserNoGraph = QueryParser.create();
        String queryString = "match $x isa movie select $x";
        MatchQuery query = queryParserNoGraph.parseMatchQuery("match $x isa movie select $x").getMatchQuery();
        assertEquals(queryString, query.toString());
        assertTrue(query.withTransaction(transaction).stream().findAny().isPresent());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBadSyntaxThrowsIllegalArgumentException() {
        qp.parseMatchQuery("match");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMultipleQueriesThrowsIllegalArgumentException() {
        qp.parseInsertQuery("insert $x isa movie; insert $y isa movie").execute();
    }

    private void assertOrderedQueriesEqual(MatchQuery query, MatchQueryPrinter parsedQuery) {
        assertEquals(
                Lists.newArrayList(query).toString(),
                Lists.newArrayList(parsedQuery.getMatchQuery()).toString()
        );
    }

    public static void assertQueriesEqual(MatchQuery query, MatchQueryPrinter parsedQuery) {
        assertEquals(Sets.newHashSet(query), Sets.newHashSet(parsedQuery.getMatchQuery()));
    }
}