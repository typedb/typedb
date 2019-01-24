/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.parser;

import com.google.common.base.Strings;
import grakn.core.graql.exception.GraqlSyntaxException;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.query.AggregateQuery;
import grakn.core.graql.query.ComputeQuery;
import grakn.core.graql.query.DefineQuery;
import grakn.core.graql.query.DeleteQuery;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.GroupAggregateQuery;
import grakn.core.graql.query.GroupQuery;
import grakn.core.graql.query.InsertQuery;
import grakn.core.graql.query.Query;
import grakn.core.graql.query.UndefineQuery;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.property.DataTypeProperty;
import grakn.core.graql.query.pattern.statement.Statement;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static grakn.core.graql.query.ComputeQuery.Algorithm.CONNECTED_COMPONENT;
import static grakn.core.graql.query.ComputeQuery.Algorithm.K_CORE;
import static grakn.core.graql.query.ComputeQuery.Argument.k;
import static grakn.core.graql.query.ComputeQuery.Argument.size;
import static grakn.core.graql.query.ComputeQuery.Method.CLUSTER;
import static grakn.core.graql.query.Graql.and;
import static grakn.core.graql.query.Graql.contains;
import static grakn.core.graql.query.Graql.define;
import static grakn.core.graql.query.Graql.eq;
import static grakn.core.graql.query.Graql.gt;
import static grakn.core.graql.query.Graql.gte;
import static grakn.core.graql.query.Graql.insert;
import static grakn.core.graql.query.Graql.like;
import static grakn.core.graql.query.Graql.lt;
import static grakn.core.graql.query.Graql.lte;
import static grakn.core.graql.query.Graql.match;
import static grakn.core.graql.query.Graql.neq;
import static grakn.core.graql.query.Graql.or;
import static grakn.core.graql.query.Graql.parse;
import static grakn.core.graql.query.Graql.rel;
import static grakn.core.graql.query.Graql.type;
import static grakn.core.graql.query.Graql.undefine;
import static grakn.core.graql.query.Graql.var;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ParserTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    // Some of these tests execute against infinite or very large input.
    // Therefore we add a (generous) timeout so we can tell if something is wrong sooner.
    @ClassRule
    public static final Timeout timeout = Timeout.seconds(60 * 5);

    private void assertQueryEquals(Query expected, Query parsed, String query) {
        assertEquals(expected, parsed);
        assertEquals(expected, Graql.parse(parsed.toString()));
        assertEquals(query, expected.toString());
    }

    private void assertQueryEquals(Pattern expected, Pattern parsed, String query) {
        assertEquals(expected, parsed);
        assertEquals(expected, Graql.parsePattern(parsed.toString()));
        assertEquals(query, expected.toString());
    }

    @Test
    public void testSimpleQuery() {
        String query = "match $x isa movie; get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(var("x").isa("movie")).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testRelationQuery() {
        String query = "match\n" +
                "$brando 'Marl B' isa name;\n" +
                "(actor: $brando, $char, production-with-cast: $prod);\n" +
                "get $char, $prod;";
        GetQuery parsed = parse(query);

        GetQuery expected = match(
                var("brando").val("Marl B").isa("name"),
                rel("actor", "brando").rel("char").rel("production-with-cast", "prod")
        ).get("char", "prod");

        assertQueryEquals(expected, parsed, query.replace("'", "\""));
    }

    @Test
    public void testPredicateQuery1() {
        String query = "match\n" +
                "$x isa movie, has title $t;\n" +
                "{ $t 'Apocalypse Now'; } or { $t < 'Juno'; $t > 'Godfather'; } or { $t 'Spy'; };\n" +
                "$t !== 'Apocalypse Now';\n" +
                "get;";
        GetQuery parsed = parse(query);

        GetQuery expected = match(
                var("x").isa("movie").has("title", var("t")),
                or(
                        var("t").val(eq("Apocalypse Now")),
                        and(var("t").val(lt("Juno")), var("t").val(gt("Godfather"))),
                        var("t").val(eq("Spy"))
                ),
                var("t").val(neq("Apocalypse Now"))
        ).get();

        assertQueryEquals(expected, parsed, query.replace("'", "\""));
    }

    @Test
    public void testPredicateQuery2() {
        String query = "match\n" +
                "$x isa movie, has title $t;\n" +
                "{ $t <= 'Juno'; $t >= 'Godfather'; $t !== 'Heat'; } or { $t 'The Muppets'; };\n" +
                "get;";
        GetQuery parsed = parse(query);

        GetQuery expected = match(
                var("x").isa("movie").has("title", var("t")),
                or(
                        and(
                                var("t").val(lte("Juno")),
                                var("t").val(gte("Godfather")),
                                var("t").val(neq("Heat"))
                        ),
                        var("t").val("The Muppets")
                )
        ).get();

        assertQueryEquals(expected, parsed, query.replace("'", "\""));
    }

    @Test
    public void testPredicateQuery3() {
        String query = "match\n" +
                "($x, $y);\n" +
                "$y isa person, has name $n;\n" +
                "{ $n contains 'ar'; } or { $n like '^M.*$'; };\n" +
                "get;";
        GetQuery parsed = parse(query);

        GetQuery expected = match(
                rel("x").rel("y"),
                var("y").isa("person").has("name", var("n")),
                or(var("n").val(contains("ar")), var("n").val(like("^M.*$")))
        ).get();

        assertQueryEquals(expected, parsed, query.replace("'", "\""));
    }

    @Test
    public void testPredicateQuery4() {
        String query = "match\n" +
                "$x has age $y;\n" +
                "$y >= $z;\n" +
                "$z 18 isa age;\n" +
                "get;";
        GetQuery parsed = parse(query);

        GetQuery expected = match(
                var("x").has("age", var("y")),
                var("y").val(gte(var("z"))),
                var("z").val(18).isa("age")
        ).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void whenParsingContainsPredicateWithAVariable_ResultMatchesJavaGraql() {
        String query = "match $x contains $y; get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(var("x").val(contains(var("y")))).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testValueEqualsVariableQuery() {
        String query = "match $s1 == $s2; get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(var("s1").val(var("s2"))).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testMoviesReleasedAfterOrAtTheSameTimeAsSpy() {
        String query = "match\n" +
                "$x has release-date >= $r;\n" +
                "$_ has title 'Spy', has release-date $r;\n" +
                "get;";
        GetQuery parsed = parse(query);

        GetQuery expected = match(
                var("x").has("release-date", gte(var("r"))),
                var().has("title", "Spy").has("release-date", var("r"))
        ).get();

        assertQueryEquals(expected, parsed, query.replace("'", "\""));
    }

    @Test
    public void testPredicates() {
        String query = "match $x has release-date < 1986-03-03T00:00, has tmdb-vote-count 100, has tmdb-vote-average <= 9.0; get;";
        GetQuery parsed = parse(query);

        GetQuery expected = match(
                var("x")
                        .has("release-date", lt(LocalDate.of(1986, 3, 3).atStartOfDay()))
                        .has("tmdb-vote-count", 100)
                        .has("tmdb-vote-average", lte(9.0))
        ).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void whenParsingDate_HandleTime() {
        String query = "match $x has release-date 1000-11-12T13:14:15; get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(var("x").has("release-date", LocalDateTime.of(1000, 11, 12, 13, 14, 15))).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void whenParsingDate_HandleBigYears() {
        GetQuery expected = match(var("x").has("release-date", LocalDate.of(12345, 12, 25).atStartOfDay())).get();
        String query = "match $x has release-date +12345-12-25T00:00; get;";
        GetQuery parsed = parse(query);

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void whenParsingDate_HandleSmallYears() {
        String query = "match $x has release-date 0867-01-01T00:00; get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(var("x").has("release-date", LocalDate.of(867, 1, 1).atStartOfDay())).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void whenParsingDate_HandleNegativeYears() {
        String query = "match $x has release-date -3200-01-01T00:00; get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(var("x").has("release-date", LocalDate.of(-3200, 1, 1).atStartOfDay())).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void whenParsingDate_HandleTimeWithDecimalSeconds() {
        String query = "match $x has release-date 1000-11-12T13:14:15.000123456; get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(var("x").has("release-date", LocalDateTime.of(1000, 11, 12, 13, 14, 15, 123_456))).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testLongComparatorQuery() throws ParseException {
        String query = "match $x isa movie, has tmdb-vote-count <= 400; get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(var("x").isa("movie").has("tmdb-vote-count", lte(400))).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void whenSearchingForImplicitType_EnsureQueryCanBeParsed() {
        String query = "match $x plays @has-release-date-owner; get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(var("x").plays("@has-release-date-owner")).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test @Ignore
    public void testModifierQuery() {
        String query = "match $y isa movie, has title $n; order by $n; limit 4; offset 2; get;" ;
        GetQuery parsed = parse(query);
        GetQuery expected = match(var("y").isa("movie").has("title", var("n"))).get();
        // TODO: put back .orderBy("n").limit(4).offset(2)

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testSchemaQuery() {
        String query = "match $x plays actor; get;"; // TODO: put back order by $x asc;
        GetQuery parsed = parse(query);
        GetQuery expected = match(var("x").plays("actor")).get(); // TODO: put back .orderBy("x")

        assertQueryEquals(expected, parsed, query);
    }

    @Test @Ignore // TODO: put back .orderBy("r", desc)
    public void testOrderQuery() {
        String query = "match $x isa movie, has release-date $r; order by $r desc; get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(var("x").isa("movie").has("release-date", var("r"))).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testVariablesEverywhereQuery() {
        String query = "match\n" +
                "($p: $x, $y);\n" +
                "$x isa $z;\n" +
                "$y 'crime';\n" +
                "$z sub production;\n" +
                "has-genre relates $p;\n" +
                "get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(
                rel(var("p"), var("x")).rel("y"),
                var("x").isa(var("z")),
                var("y").val("crime"),
                var("z").sub("production"),
                type("has-genre").relates(var("p"))
        ).get();

        assertQueryEquals(expected, parsed, query.replace("'", "\""));
    }

    @Test
    public void testParseRelatesTypeVariable() {
        String query = "match\n" +
                "$x isa $type;\n" +
                "$type relates someRole;\n" +
                "get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(var("x").isa(var("type")), var("type").relates("someRole")).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testOrQuery() {
        String query = "match\n" +
                "$x isa movie;\n" +
                "{ $y 'drama' isa genre; ($x, $y); } or { $x 'The Muppets'; };\n" +
                "get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(
                var("x").isa("movie"),
                or(
                        and(
                                var("y").val("drama").isa("genre"),
                                rel("x").rel("y")
                        ),
                        var("x").val("The Muppets")
                )
        ).get();

        assertQueryEquals(expected, parsed, query.replace("'", "\""));
    }

    @Test
    public void testAggregateCountQuery() {
        String query = "match ($x, $y) isa friendship; get $x, $y; count;";
        AggregateQuery parsed = parse(query);
        AggregateQuery expected = match(rel("x").rel("y").isa("friendship")).get("x", "y").count();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testAggregateGroupCountQuery() {
        String query = "match ($x, $y) isa friendship; get $x, $y; group $x; count;";
        GroupAggregateQuery parsed = parse(query);
        GroupAggregateQuery expected = match(rel("x").rel("y").isa("friendship")).get("x", "y").group("x").count();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testAggregateGroupMaxQuery() {
        String query = "match\n" +
                "($x, $y) isa friendship;\n" +
                "$y has age $z;\n" +
                "get; group $x; max $z;";
        GroupAggregateQuery parsed = parse(query);
        GroupAggregateQuery expected = match(
                rel("x").rel("y").isa("friendship"),
                var("y").has("age", var("z"))
        ).get().group("x").max("z");

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void whenComparingCountQueryUsingGraqlAndJavaGraql_TheyAreEquivalent() {
        String query = "match $x isa movie, has title \"Godfather\"; get; count;";
        AggregateQuery parsed = parse(query);
        AggregateQuery expected = match(var("x").isa("movie").has("title", "Godfather")).get().count();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testInsertQuery() {
        String query = "insert $_ isa movie, has title \"The Title\";";
        InsertQuery parsed = parse(query);
        InsertQuery expected = insert(var().isa("movie").has("title", "The Title"));

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void whenParsingDeleteQuery_ResultIsSameAsJavaGraql() {
        String query = "match\n" +
                "$x isa movie, has title 'The Title';\n" +
                "$y isa movie;\n" +
                "delete $x, $y;";
        DeleteQuery parsed = parse(query);
        DeleteQuery expected = match(
                var("x").isa("movie").has("title", "The Title"),
                var("y").isa("movie")
        ).delete("x", "y");

        assertQueryEquals(expected, parsed, query.replace("'", "\""));
    }

    @Test
    public void whenParsingDeleteQueryWithNoArguments_ResultIsSameAsJavaGraql() {
        String query = "match\n" +
                "$x isa movie, has title 'The Title';\n" +
                "$y isa movie;\n" +
                "delete;";
        DeleteQuery parsed = parse(query);
        DeleteQuery expected = match(
                var("x").isa("movie").has("title", "The Title"),
                var("y").isa("movie")
        ).delete();

        assertQueryEquals(expected, parsed, query.replace("'", "\""));
    }

    @Test
    public void whenParsingInsertQuery_ResultIsSameAsJavaGraql() {
        String query = "insert\n" +
                "$x isa pokemon, has name 'Pichu';\n" +
                "$y isa pokemon, has name 'Pikachu';\n" +
                "$z isa pokemon, has name 'Raichu';\n" +
                "(evolves-from: $x, evolves-to: $y) isa evolution;\n" +
                "(evolves-from: $y, evolves-to: $z) isa evolution;";
        InsertQuery parsed = parse(query);
        InsertQuery expected = insert(
                var("x").has("name", "Pichu").isa("pokemon"),
                var("y").has("name", "Pikachu").isa("pokemon"),
                var("z").has("name", "Raichu").isa("pokemon"),
                rel("evolves-from", "x").rel("evolves-to", "y").isa("evolution"),
                rel("evolves-from", "y").rel("evolves-to", "z").isa("evolution")
        );

        assertQueryEquals(expected, parsed, query.replace("'", "\""));
    }

    @Test
    public void whenParsingAsInDefine_ResultIsSameAsSub() {
        String query = "define\n" +
                "parent sub role;\n" +
                "child sub role;\n" +
                "parenthood sub relationship, relates parent, relates child;\n" +
                "fatherhood sub parenthood, relates father as parent, relates son as child;";
        DefineQuery parsed = parse(query);

        DefineQuery expected = define(
                type("parent").sub("role"),
                type("child").sub("role"),
                type("parenthood").sub("relationship")
                        .relates(var().type("parent"))
                        .relates(var().type("child")),
                type("fatherhood").sub("parenthood")
                        .relates(type("father"), type("parent"))
                        .relates(type("son"), type("child"))
        );

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void whenParsingAsInMatch_ResultIsSameAsSub() {
        String query = "match fatherhood sub parenthood, relates father as parent, relates son as child; get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(
                type("fatherhood").sub("parenthood")
                        .relates(type("father"), type("parent"))
                        .relates(type("son"), type("child"))
        ).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void whenParsingDefineQuery_ResultIsSameAsJavaGraql() {
        String query = "define\n" +
                "pokemon sub entity;\n" +
                "evolution sub " + Schema.MetaSchema.RELATIONSHIP.getLabel() + ";\n" +
                "evolves-from sub role;\n" +
                "evolves-to sub role;\n" +
                "evolution relates evolves-from, relates evolves-to;\n" +
                "pokemon plays evolves-from, plays evolves-to, has name;";
        DefineQuery parsed = parse(query);

        DefineQuery expected = define(
                type("pokemon").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                type("evolution").sub(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()),
                type("evolves-from").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                type("evolves-to").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                type("evolution").relates("evolves-from").relates("evolves-to"),
                type("pokemon").plays("evolves-from").plays("evolves-to").has("name")
        );

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void whenParsingUndefineQuery_ResultIsSameAsJavaGraql() {
        String query = "undefine\n" +
                "pokemon sub entity;\n" +
                "evolution sub " + Schema.MetaSchema.RELATIONSHIP.getLabel() + ";\n" +
                "evolves-from sub role;\n" +
                "evolves-to sub role;\n" +
                "evolution relates evolves-from, relates evolves-to;\n" +
                "pokemon plays evolves-from, plays evolves-to, has name;";
        UndefineQuery parsed = parse(query);

        UndefineQuery expected = undefine(
                type("pokemon").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                type("evolution").sub(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()),
                type("evolves-from").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                type("evolves-to").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                type("evolution").relates("evolves-from").relates("evolves-to"),
                type("pokemon").plays("evolves-from").plays("evolves-to").has("name")
        );

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testMatchInsertQuery() {
        String query = "match $x isa language;\n" +
                "insert $x has name \"HELLO\";";
        InsertQuery parsed = parse(query);
        InsertQuery expected = match(var("x").isa("language"))
                .insert(var("x").has("name", "HELLO"));

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testDefineAbstractEntityQuery() {
        String query = "define\n" +
                "concrete-type sub entity;\n" +
                "abstract-type sub entity, abstract;";
        DefineQuery parsed = parse(query);
        DefineQuery expected = define(
                type("concrete-type").sub("entity"),
                type("abstract-type").sub("entity").isAbstract()
        );

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testMatchDataTypeQuery() {
        String query = "match $x datatype double; get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(var("x").datatype(Query.DataType.DOUBLE)).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testParseWithoutVar() {
        String query = "match $_ isa person; get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(var().isa("person")).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void whenParsingDateKeyword_ParseAsTheCorrectDataType() {
        String query = "match $x datatype date; get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(var("x").datatype(Query.DataType.DATE)).get();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testDefineDataTypeQuery() {
        String query = "define my-type sub attribute, datatype long;";
        DefineQuery parsed = parse(query);
        DefineQuery expected = define(type("my-type").sub("attribute").datatype(Query.DataType.LONG));

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testEscapeString() {
        String unescaped = "This has \"double quotes\" and a single-quoted backslash: '\\'";
        String escaped = "This has \\\"double quotes\\\" and a single-quoted backslash: \\'\\\\\\'";

        String query = "insert $_ isa movie, has title \"" + escaped + "\";";
        InsertQuery parsed = parse(query);
        InsertQuery expected = insert(var().isa("movie").has("title", unescaped));

        assertQueryEquals(expected, parsed, query);
    }


    @Test
    public void whenParsingQueryWithComments_TheyAreIgnored() {
        AggregateQuery expected = match(var("x").isa("movie")).get().count();
        String query = "match \n# there's a comment here\n$x isa###WOW HERES ANOTHER###\r\nmovie; get; count;";
        AggregateQuery parsed = parse(query);

        assertEquals(expected, parsed);
        assertEquals(expected, parse(parsed.toString()));
    }

    @Test
    public void testParsingPattern() {
        String pattern = "{ (wife: $a, husband: $b) isa marriage; $a has gender 'male'; $b has gender 'female'; };";
        Pattern parsed = Graql.parsePattern(pattern);
        Pattern expected = Graql.and(
                rel("wife", "a").rel("husband", "b").isa("marriage"),
                var("a").has("gender", "male"),
                var("b").has("gender", "female")
        );

        assertQueryEquals(expected, parsed, pattern.replace("'", "\""));
    }

    @Test
    public void testDefineRules() {
        String when = "$x isa movie;";
        String then = "$x has genre 'drama';";
        Pattern whenPattern = and(var("x").isa("movie"));
        Pattern thenPattern = and(var("x").has("genre", "drama"));

        DefineQuery expected = define(type("all-movies-are-drama").sub("rule").when(whenPattern).then(thenPattern));
        String query = "define all-movies-are-drama sub rule, when { " + when + " }, then { " + then + " };";
        DefineQuery parsed = parse(query);

        assertQueryEquals(expected, parsed, query.replace("'", "\""));
    }

    @Test
    public void testQueryParserWithoutGraph() {
        String queryString = "match $x isa movie; get $x;";
        GetQuery query = parse("match $x isa movie; get $x;");
        assertEquals(queryString, query.toString());
    }

    @Test
    public void testParseBoolean() {
        String query = "insert $_ has flag true;";
        InsertQuery parsed = parse(query);
        InsertQuery expected = insert(var().has("flag", true));

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testParseAggregateGroup() {
        String query = "match $x isa movie; get; group $x;";
        GroupQuery parsed = parse(query);
        GroupQuery expected = match(var("x").isa("movie")).get().group("x");

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testParseAggregateGroupCount() {
        String query = "match $x isa movie; get; group $x; count;";
        GroupAggregateQuery parsed = parse(query);
        GroupAggregateQuery expected = match(var("x").isa("movie")).get().group("x").count();

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testParseAggregateStd() {
        String query = "match $x isa movie; get; std $x;";
        AggregateQuery parsed = parse(query);
        AggregateQuery expected = match(var("x").isa("movie")).get().std( "x");

        assertQueryEquals(expected, parsed, query);
    }

    @Test
    public void testParseAggregateToString() {
        String query = "match $x isa movie; get $x; group $x; count;";
        assertEquals(query, parse(query).toString());
    }

    // ===============================================================================================================//
    // Test Graql Compute queries
    // ===============================================================================================================//
    @Test
    public void testParseComputeCount() {
        assertParseEquivalence("compute count;");
    }

    @Test
    public void testParseComputeCountWithSubgraph() {
        assertParseEquivalence("compute count in [movie, person];");
    }

    @Test
    public void testParseComputeClusterUsingCC() {
        assertParseEquivalence("compute cluster in [movie, person], using connected-component;");
    }

    @Test
    public void testParseComputeClusterUsingCCWithSize() {
        ComputeQuery expected = Graql.compute(CLUSTER).using(CONNECTED_COMPONENT).in("movie", "person").where(size(10));
        ComputeQuery parsed = Graql.parse(
                "compute cluster in [movie, person], using connected-component, where [size = 10];");

        assertEquals(expected, parsed);
    }

    @Test
    public void testParseComputeClusterUsingCCWithSizeTwice() {
        ComputeQuery expected =
                Graql.compute(CLUSTER).using(CONNECTED_COMPONENT).in("movie", "person").where(size(10), size(15));

        ComputeQuery parsed = Graql.parse(
                "compute cluster in [movie, person], using connected-component, where [size = 10, size = 15];");

        assertEquals(expected, parsed);
    }

    @Test
    public void testParseComputeClusterUsingKCore() {
        assertParseEquivalence("compute cluster in [movie, person], using k-core;");
    }

    @Test
    public void testParseComputeClusterUsingKCoreWithK() {
        ComputeQuery expected = Graql.compute(CLUSTER).using(K_CORE).in("movie", "person").where(k(10));
        ComputeQuery parsed = Graql.parse(
                "compute cluster in [movie, person], using k-core, where k = 10;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testParseComputeClusterUsingKCoreWithKTwice() {
        ComputeQuery expected = Graql.compute(CLUSTER).using(K_CORE).in("movie", "person").where(k(10));
        ComputeQuery parsed = Graql.parse(
                "compute cluster in [movie, person], using k-core, where [k = 5, k = 10];");

        assertEquals(expected, parsed);
    }

    @Test
    public void testParseComputeDegree() {
        assertParseEquivalence("compute centrality in movie, using degree;");
    }

    @Test
    public void testParseComputeCoreness() {
        assertParseEquivalence("compute centrality in movie, using k-core, where min-k=3;");
    }

    @Test
    public void testParseComputeMax() {
        assertParseEquivalence("compute max of person, in movie;");
    }

    @Test
    public void testParseComputeMean() {
        assertParseEquivalence("compute mean of person, in movie;");
    }

    @Test
    public void testParseComputeMedian() {
        assertParseEquivalence("compute median of person, in movie;");
    }

    @Test
    public void testParseComputeMin() {
        assertParseEquivalence("compute min of movie, in person;");
    }

    @Test
    public void testParseComputePath() {
        assertParseEquivalence("compute path from \"1\", to \"2\", in person;");
    }

    @Test
    public void testParseComputePathWithMultipleInTypes() {
        assertParseEquivalence("compute path from \"1\", to \"2\", in [person, marriage];");
    }

    @Test
    public void testParseComputeStd() {
        assertParseEquivalence("compute std of movie;");
    }

    @Test
    public void testParseComputeSum() {
        assertParseEquivalence("compute sum of movie, in person;");
    }

    // ===============================================================================================================//


    @Test
    public void whenParseIncorrectSyntax_ThrowGraqlSyntaxExceptionWithHelpfulError() {
        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage(allOf(
                containsString("syntax error"), containsString("line 1"),
                containsString("\nmatch $x isa "),
                containsString("\n             ^")
        ));
        //noinspection ResultOfMethodCallIgnored
        parse("match $x isa ");
    }

    @Test
    public void whenParseIncorrectSyntax_ErrorMessageShouldRetainWhitespace() {
        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage(not(containsString("match$xisa")));
        //noinspection ResultOfMethodCallIgnored
        parse("match $x isa ");
    }

    @Test
    public void testSyntaxErrorPointer() {
        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage(allOf(
                containsString("\nmatch $x is"),
                containsString("\n         ^")
        ));
        //noinspection ResultOfMethodCallIgnored
        parse("match $x is");
    }

    @Test
    public void testHasVariable() {
        String query = "match $_ has title 'Godfather', has tmdb-vote-count $x; get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(var().has("title", "Godfather").has("tmdb-vote-count", var("x"))).get();

        assertQueryEquals(expected, parsed, query.replace("'", "\""));
    }

    @Test
    public void testRegexAttributeType() {
        String query = "match $x regex '(fe)?male'; get;";
        GetQuery parsed = parse(query);
        GetQuery expected = match(var("x").regex("(fe)?male")).get();

        assertQueryEquals(expected, parsed, query.replace("'", "\""));
    }

    @Test
    public void testGraqlParseQuery() {
        assertTrue(parse("match $x isa movie; get;") instanceof GetQuery);
    }

    @Test
    public void testParseBooleanType() {
        GetQuery query = parse("match $x datatype boolean; get;");

        Statement var = query.match().getPatterns().statements().iterator().next();

        //noinspection OptionalGetWithoutIsPresent
        DataTypeProperty property = var.getProperty(DataTypeProperty.class).get();

        Assert.assertEquals(Query.DataType.BOOLEAN, property.dataType());
    }

    @Test
    public void testParseKey() {
        assertEquals("match $x key name; get $x;", parse("match $x key name; get $x;").toString());
    }

    @Test
    public void testParseListEmpty() {
        List<Query> queries = Graql.parseList("").collect(toList());
        assertEquals(0, queries.size());
    }

    @Test
    public void testParseListOneMatch() {
        String getString = "match $y isa movie; get;";
        List<Query> queries = Graql.parseList(getString).collect(toList());

        assertEquals(Arrays.asList(match(var("y").isa("movie")).get()), queries);
    }

    @Test
    public void testParseListOneInsert() {
        String insertString = "insert $x isa movie;";
        List<Query> queries = Graql.parseList(insertString).collect(toList());

        assertEquals(Arrays.asList(insert(var("x").isa("movie"))), queries);
    }

    @Test
    public void testParseListOneInsertWithWhitespacePrefix() {
        String insertString = " insert $x isa movie;";
        List<Query> queries = Graql.parseList(insertString).collect(toList());

        assertEquals(Arrays.asList(insert(var("x").isa("movie"))), queries);
    }

    @Test
    public void testParseListOneInsertWithPrefixComment() {
        String insertString = "#hola\ninsert $x isa movie;";
        List<Query> queries = Graql.parseList(insertString).collect(toList());

        assertEquals(Arrays.asList(insert(var("x").isa("movie"))), queries);
    }

    @Test
    public void testParseList() {
        String insertString = "insert $x isa movie;";
        String getString = "match $y isa movie; get;";
        List<Query> queries = Graql.parseList(insertString + getString).collect(toList());

        assertEquals(Arrays.asList(insert(var("x").isa("movie")), match(var("y").isa("movie")).get()), queries);
    }

    @Test
    public void testParseListMatchInsert() {
        String matchString = "match $y isa movie;";
        String insertString = "insert $x isa movie;";
        List<Query> queries = Graql.parseList(matchString + insertString).collect(toList());

        assertEquals(Arrays.asList(match(var("y").isa("movie")).insert(var("x").isa("movie"))), queries);
    }

    @Test
    public void testParseMatchInsertBeforeAndAfter() {
        String matchString = "match $y isa movie;";
        String insertString = "insert $x isa movie;";
        String getString = matchString + " get;";
        String matchInsert = matchString + insertString;

        List<String> options = newArrayList(
                getString + matchInsert,
                insertString + matchInsert,
                matchInsert + getString,
                matchInsert + insertString
        );

        options.forEach(option -> {
            List<Query> queries = Graql.parseList(option).collect(toList());
            assertEquals(option, 2, queries.size());
        });
    }

    @Test
    public void testParseManyMatchInsertWithoutStackOverflow() {
        int numQueries = 10_000;
        String matchInsertString = "match $x isa person; insert $y isa person;\n";
        String longQueryString = Strings.repeat(matchInsertString, numQueries);
        InsertQuery matchInsert = match(var("x").isa("person")).insert(var("y").isa("person"));

        List<InsertQuery> queries = Graql.<InsertQuery>parseList(longQueryString).collect(toList());

        assertEquals(Collections.nCopies(numQueries, matchInsert), queries);
    }

    @Test
    public void whenParsingAListOfQueriesWithASyntaxError_ReportError() {
        String queryText = "define person sub entity has name;"; // note no semicolon

        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage("define person sub entity has name;"); // Message should refer to line

        //noinspection ResultOfMethodCallIgnored
        Graql.parse(queryText);
    }

    @Test
    public void whenParsingAQueryWithReifiedAttributeRelationshipSyntax_ItIsEquivalentToJavaGraql() {
        assertParseEquivalence("match $x has name $z via $x; get $x;");
    }

    @SuppressWarnings("CheckReturnValue")
    @Test(expected = GraqlSyntaxException.class)
    public void whenParsingMultipleQueriesLikeOne_Throw() {
        //noinspection ResultOfMethodCallIgnored
        parse("insert $x isa movie; insert $y isa movie");
    }

    @Test
    public void testMissingColon() {
        exception.expect(GraqlSyntaxException.class);
        //noinspection ResultOfMethodCallIgnored
        parse("match (actor $x, $y) isa has-cast; get;");
    }

    @Test
    public void testMissingComma() {
        exception.expect(GraqlSyntaxException.class);
        //noinspection ResultOfMethodCallIgnored
        parse("match ($x $y) isa has-cast; get;");
    }

    @Test
    public void testLimitMistake() {
        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage("limit1");
        //noinspection ResultOfMethodCallIgnored
        parse("match ($x, $y); limit1;");
    }

    @Test
    public void whenParsingAggregateWithWrongVariableArgumentNumber_Throw() {
        exception.expect(GraqlSyntaxException.class);
        //noinspection ResultOfMethodCallIgnored
        parse("match $x isa name; get; group;");
    }

    @Test
    public void whenParsingAggregateWithWrongName_Throw() {
        exception.expect(GraqlSyntaxException.class);
        //noinspection ResultOfMethodCallIgnored
        parse("match $x isa name; get; hello $x;");
    }

    @Test
    public void regexPredicateParsesCharacterClassesCorrectly() {
        assertEquals(match(var("x").val(like("\\d"))).get(), parse("match $x like '\\d'; get;"));
    }

    @Test
    public void regexPredicateParsesQuotesCorrectly() {
        assertEquals(match(var("x").val(like("\""))).get(), parse("match $x like '\"'; get;"));
    }

    @Test
    public void regexPredicateParsesBackslashesCorrectly() {
        assertEquals(match(var("x").val(like("\\\\"))).get(), parse("match $x like '\\\\'; get;"));
    }

    @Test
    public void regexPredicateParsesNewlineCorrectly() {
        assertEquals(match(var("x").val(like("\\n"))).get(), parse("match $x like '\\n'; get;"));
    }

    @Test
    public void regexPredicateParsesForwardSlashesCorrectly() {
        assertEquals(match(var("x").val(like("/"))).get(), parse("match $x like '\\/'; get;"));
    }

    @Test
    public void whenValueEqualityToString_CreateValidQueryString() {
        GetQuery expected = match(var("x").val(eq(var("y")))).get();
        GetQuery parsed = Graql.parse(expected.toString());
        assertEquals(expected, parsed);
    }

    private static void assertParseEquivalence(String query) {
        assertEquals(query, parse(query).toString());
    }
}