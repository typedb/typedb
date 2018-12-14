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
import grakn.core.graql.answer.AnswerGroup;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.Value;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.exception.GraqlSyntaxException;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.query.AggregateQuery;
import grakn.core.graql.query.ComputeQuery;
import grakn.core.graql.query.DefineQuery;
import grakn.core.graql.query.DeleteQuery;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.InsertQuery;
import grakn.core.graql.query.Query;
import grakn.core.graql.query.UndefineQuery;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.DataTypeProperty;
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
import static grakn.core.graql.query.Graql.contains;
import static grakn.core.graql.query.Graql.count;
import static grakn.core.graql.query.Graql.define;
import static grakn.core.graql.query.Graql.eq;
import static grakn.core.graql.query.Graql.group;
import static grakn.core.graql.query.Graql.gt;
import static grakn.core.graql.query.Graql.gte;
import static grakn.core.graql.query.Graql.insert;
import static grakn.core.graql.query.Graql.lt;
import static grakn.core.graql.query.Graql.lte;
import static grakn.core.graql.query.Graql.match;
import static grakn.core.graql.query.Graql.max;
import static grakn.core.graql.query.Graql.neq;
import static grakn.core.graql.query.Graql.parse;
import static grakn.core.graql.query.Graql.regex;
import static grakn.core.graql.query.Graql.std;
import static grakn.core.graql.query.Graql.undefine;
import static grakn.core.graql.query.pattern.Pattern.and;
import static grakn.core.graql.query.pattern.Pattern.label;
import static grakn.core.graql.query.pattern.Pattern.or;
import static grakn.core.graql.query.pattern.Pattern.var;
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

    @Test
    public void testSimpleQuery() {
        assertEquals(
                match(var("x").isa("movie")).get(),
                parse("match $x isa movie; get;")
        );
    }

    @Test
    public void testRelationQuery() {
        GetQuery expected = match(
                var("brando").val("Marl B").isa("person"),
                var().rel("actor", "brando").rel("char").rel("production-with-cast", "prod")
        ).get("char", "prod");

        GetQuery parsed = parse(
                "match\n" +
                        "$brando == \"Marl B\" isa person;\n" +
                        "(actor: $brando, $char, production-with-cast: $prod);\n" +
                        "get $char, $prod;"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void testPredicateQuery1() {
        GetQuery expected = match(
                var("x").isa("movie").has("title", var("t")),
                or(
                        or(
                                var("t").val(eq("Apocalypse Now")),
                                and(var("t").val(lt("Juno")), var("t").val(gt("Godfather")))
                        ),
                        var("t").val(eq("Spy"))
                ),
                var("t").val(neq("Apocalypse Now"))
        ).get();

        GetQuery parsed = parse(
                "match\n" +
                        "$x isa movie, has title $t;\n" +
                        "$t == \"Apocalypse Now\" or {$t < 'Juno'; $t > 'Godfather';} or $t 'Spy';" +
                        "$t !=='Apocalypse Now'; get;\n"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void testPredicateQuery2() {
        GetQuery expected = match(
                var("x").isa("movie").has("title", var("t")),
                or(
                        and(var("t").val(lte("Juno")), var("t").val(gte("Godfather")), var("t").val(neq("Heat"))),
                        var("t").val("The Muppets")
                )
        ).get();

        GetQuery parsed = parse(
                "match $x isa movie, has title $t;" +
                        "{$t <= 'Juno'; $t >= 'Godfather'; $t !== 'Heat';} or $t == 'The Muppets'; get;"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void testPredicateQuery3() {
        GetQuery expected = match(
                var().rel("x").rel("y"),
                var("y").isa("person").has("name", var("n")),
                or(var("n").val(contains("ar")), var("n").val(regex("^M.*$")))
        ).get();

        GetQuery parsed = parse(
                "match ($x, $y); $y isa person, has name $n;" +
                        "$n contains 'ar' or $n /^M.*$/; get;"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void whenParsingContainsPredicateWithAVariable_ResultMatchesJavaGraql() {
        GetQuery expected = match(var("x").val(contains(var("y")))).get();

        GetQuery parsed = parse("match $x contains $y; get;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testValueEqualsVariableQuery() {
        GetQuery expected = match(var("s1").val(var("s2"))).get();

        GetQuery parsed = parse("match $s1 == $s2; get;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testMoviesReleasedAfterOrAtTheSameTimeAsSpy() {
        GetQuery expected = match(
                var("x").has("release-date", gte(var("r"))),
                var().has("title", "Spy").has("release-date", var("r"))
        ).get();

        GetQuery parsed = parse("match $x has release-date >= $r; has title 'Spy', has release-date $r; get;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testTypesQuery() throws ParseException {
        GetQuery expected = match(
                var("x")
                        .has("release-date", lt(LocalDate.of(1986, 3, 3).atStartOfDay()))
                        .has("tmdb-vote-count", 100)
                        .has("tmdb-vote-average", lte(9.0))
        ).get();

        GetQuery parsed = parse(
                "match $x has release-date < 1986-03-03, has tmdb-vote-count 100 has tmdb-vote-average<=9.0; get;"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void whenParsingDate_HandleTime() {
        GetQuery expected = match(var("x").has("release-date", LocalDateTime.of(1000, 11, 12, 13, 14, 15))).get();
        GetQuery parsed = parse("match $x has release-date 1000-11-12T13:14:15; get;");
        assertEquals(expected, parsed);
    }

    @Test
    public void whenParsingDate_HandleBigYears() {
        GetQuery expected = match(var("x").has("release-date", LocalDate.of(12345, 12, 25).atStartOfDay())).get();
        GetQuery parsed = parse("match $x has release-date +12345-12-25; get;");
        assertEquals(expected, parsed);
    }

    @Test
    public void whenParsingDate_HandleSmallYears() {
        GetQuery expected = match(var("x").has("release-date", LocalDate.of(867, 1, 1).atStartOfDay())).get();
        GetQuery parsed = parse("match $x has release-date 0867-01-01; get;");
        assertEquals(expected, parsed);
    }

    @Test
    public void whenParsingDate_HandleNegativeYears() {
        GetQuery expected = match(var("x").has("release-date", LocalDate.of(-3200, 1, 1).atStartOfDay())).get();
        GetQuery parsed = parse("match $x has release-date -3200-01-01; get;");
        assertEquals(expected, parsed);
    }

    @Test
    public void whenParsingDate_HandleTimeWithDecimalSeconds() {
        GetQuery expected = match(var("x").has("release-date", LocalDateTime.of(1000, 11, 12, 13, 14, 15, 123_456))).get();
        GetQuery parsed = parse("match $x has release-date 1000-11-12T13:14:15.000123456; get;");
        assertEquals(expected, parsed);
    }

    @Test
    public void testLongComparatorQuery() throws ParseException {
        GetQuery expected = match(
                var("x").isa("movie").has("tmdb-vote-count", lte(400))
        ).get();

        GetQuery parsed = parse("match $x isa movie, has tmdb-vote-count <= 400; get;");

        assertEquals(expected, parsed);
    }

    @Test
    public void whenSearchingForImplicitType_EnsureQueryCanBeParsed() {
        GetQuery expected = match(
                var("x").plays("@has-release-date-owner")
        ).get();

        GetQuery parsed = parse("match $x plays @has-release-date-owner; get;");

        assertEquals(expected, parsed);
    }

    @Test @Ignore
    public void testModifierQuery() {
        GetQuery expected = match(
                var("y").isa("movie").has("title", var("n"))
        ).get(); // TODO: put back .orderBy("n").limit(4).offset(2)

        GetQuery parsed =
                parse("match $y isa movie, has title $n; order by $n; limit 4; offset 2; get;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testSchemaQuery() {
        GetQuery expected = match(var("x").plays("actor")).get(); // TODO: put back .orderBy("x")
        GetQuery parsed = parse("match $x plays actor; get;"); // TODO: put back order by $x asc;
        assertEquals(expected, parsed);
    }

    @Test @Ignore
    public void testOrderQuery() {
        // TODO: put back .orderBy("r", desc)
        GetQuery expected = match(var("x").isa("movie").has("release-date", var("r"))).get();
        GetQuery parsed = parse("match $x isa movie, has release-date $r; order by $r desc; get;");
        assertEquals(expected, parsed);
    }

    @Test
    public void testVariablesEverywhereQuery() {
        GetQuery expected = match(
                var().rel(var("p"), "x").rel("y"),
                var("x").isa(var("z")),
                var("y").val("crime"),
                var("z").sub("production"),
                label("has-genre").relates(var("p"))
        ).get();

        GetQuery parsed = parse(
                "match" +
                        "($p: $x, $y);" +
                        "$x isa $z;" +
                        "$y == 'crime';" +
                        "$z sub production;" +
                        "has-genre relates $p; get;"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void testOrQuery() {
        GetQuery expected = match(
                var("x").isa("movie"),
                or(
                        and(var("y").isa("genre").val("drama"), var().rel("x").rel("y")),
                        var("x").val("The Muppets")
                )
        ).get();

        GetQuery parsed = parse(
                "match $x isa movie; { $y isa genre == 'drama'; ($x, $y); } or $x == 'The Muppets'; get;"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void testAggregateCountQuery() {
        AggregateQuery<Value> expected = match(var().rel("x").rel("y").isa("friendship")).aggregate(count("x", "y"));
        AggregateQuery<Value> parsed = parse("match ($x, $y) isa friendship; aggregate count $x, $y;");
        assertEquals(expected, parsed);
    }

    @Test
    public void testAggregateGroupCountQuery() {
        AggregateQuery<AnswerGroup<Value>> expected = match(var().rel("x").rel("y").isa("friendship")).aggregate(group("x", count("x", "y")));
        AggregateQuery<AnswerGroup<Value>> parsed = parse("match ($x, $y) isa friendship; aggregate group $x, count $x, $y;");
        assertEquals(expected, parsed);
    }

    @Test
    public void testAggregateGroupMaxQuery() {
        AggregateQuery<AnswerGroup<Value>> expected =
                match(
                        var().rel("x").rel("y").isa("friendship"),
                        var("y").has("age", var("z"))
                ).aggregate(
                        group("x", max("z"))
                );
        AggregateQuery<AnswerGroup<Value>> parsed = parse("match ($x, $y) isa friendship; $y has age $z; aggregate group $x, max $z;");
        assertEquals(expected, parsed);
    }

    @Test
    public void whenComparingCountQueryUsingGraqlAndJavaGraql_TheyAreEquivalent() {
        AggregateQuery<Value> expected = match(var("x").isa("movie").has("title", "Godfather")).aggregate(count());
        AggregateQuery<Value> parsed = parse("match $x isa movie has title 'Godfather'; aggregate count;");
        assertEquals(expected, parsed);
    }

    @Test
    public void testInsertQuery() {
        InsertQuery expected = insert(var().isa("movie").has("title", "The Title"));
        InsertQuery parsed = parse("insert isa movie has title 'The Title';");
        assertEquals(expected, parsed);
    }

    @Test
    public void whenParsingDeleteQuery_ResultIsSameAsJavaGraql() {
        Variable x = var("x");
        Variable y = var("y");

        DeleteQuery expected = match(x.isa("movie").has("title", "The Title"), y.isa("movie")).delete(x, y);
        DeleteQuery parsed = parse("match $x isa movie has title 'The Title'; $y isa movie; delete $x, $y;");
        assertEquals(expected, parsed);
    }

    @Test
    public void whenParsingDeleteQueryWithNoArguments_ResultIsSameAsJavaGraql() {
        DeleteQuery expected = match(var("x").isa("movie").has("title", "The Title"), var("y").isa("movie")).delete();
        DeleteQuery parsed = parse("match $x isa movie has title 'The Title'; $y isa movie; delete;");
        assertEquals(expected, parsed);
    }

    @Test
    public void whenParsingInsertQuery_ResultIsSameAsJavaGraql() {
        InsertQuery expected = insert(
                var("x").has("name", "Pichu").isa("pokemon"),
                var("y").has("name", "Pikachu").isa("pokemon"),
                var("z").has("name", "Raichu").isa("pokemon"),
                var().rel("evolves-from", "x").rel("evolves-to", "y").isa("evolution"),
                var().rel("evolves-from", "y").rel("evolves-to", "z").isa("evolution")
        );

        InsertQuery parsed = parse("insert " +
                                           "$x has name 'Pichu' isa pokemon;" +
                                           "$y has name 'Pikachu' isa pokemon;" +
                                           "$z has name 'Raichu' isa pokemon;" +
                                           "(evolves-from: $x ,evolves-to: $y) isa evolution;" +
                                           "(evolves-from: $y, evolves-to: $z) isa evolution;"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void whenParsingAsInDefine_ResultIsSameAsSub() {
        DefineQuery expected = define(
                label("parent").sub("role"),
                label("child").sub("role"),
                label("parenthood").sub("relationship")
                        .relates(var().label("parent"))
                        .relates(var().label("child")),
                label("fatherhood").sub("parenthood")
                        .relates(label("father"), label("parent"))
                        .relates(label("son"), label("child"))
        );

        DefineQuery parsed = parse("define " +
                                           "parent sub role;\n" +
                                           "child sub role;\n" +
                                           "parenthood sub relationship, relates parent, relates child;\n" +
                                           "fatherhood sub parenthood, relates father as parent, relates son as child;"
        );

        assertEquals(expected, parsed);
        assertEquals(expected, parse(expected.toString()));
    }

    @Test
    public void whenParsingAsInMatch_ResultIsSameAsSub() {
        GetQuery expected = match(
                label("fatherhood").sub("parenthood")
                        .relates(var("x"), label("parent"))
                        .relates(label("son"), var("y"))
        ).get();

        GetQuery parsed = parse("match " +
                                        "fatherhood sub parenthood, relates $x as parent, relates son as $y; get;"
        );

        assertEquals(expected, parsed);
        assertEquals(expected, parse(expected.toString()));
    }

    @Test
    public void whenParsingDefineQuery_ResultIsSameAsJavaGraql() {
        DefineQuery expected = define(
                label("pokemon").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                label("evolution").sub(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()),
                label("evolves-from").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("evolves-to").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("evolution").relates("evolves-from").relates("evolves-to"),
                label("pokemon").plays("evolves-from").plays("evolves-to").has("name")
        );

        DefineQuery parsed = parse("define " +
                                           "'pokemon' sub entity;" +
                                           "evolution sub " + Schema.MetaSchema.RELATIONSHIP.getLabel() + ";" +
                                           "evolves-from sub role;" +
                                           "label \"evolves-to\" sub role;" +
                                           "evolution relates evolves-from, relates evolves-to;" +
                                           "pokemon plays evolves-from plays evolves-to has name;"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void whenParsingUndefineQuery_ResultIsSameAsJavaGraql() {
        UndefineQuery expected = undefine(
                label("pokemon").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                label("evolution").sub(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()),
                label("evolves-from").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("evolves-to").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("evolution").relates("evolves-from").relates("evolves-to"),
                label("pokemon").plays("evolves-from").plays("evolves-to").has("name")
        );

        UndefineQuery parsed = parse("undefine " +
                                             "'pokemon' sub entity;" +
                                             "evolution sub " + Schema.MetaSchema.RELATIONSHIP.getLabel() + ";" +
                                             "evolves-from sub role;" +
                                             "label \"evolves-to\" sub role;" +
                                             "evolution relates evolves-from, relates evolves-to;" +
                                             "pokemon plays evolves-from plays evolves-to has name;"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void testMatchInsertQuery() {
        InsertQuery expected = match(var("x").isa("language")).insert(var("x").has("name", "HELLO"));
        InsertQuery parsed = parse("match $x isa language; insert $x has name \"HELLO\";");
        assertEquals(expected, parsed);
    }

    @Test
    public void testInsertIsAbstractQuery() {
        InsertQuery expected = insert(
                label("concrete-type").sub("entity"),
                label("abstract-type").isAbstract().sub("entity")
        );

        InsertQuery parsed = parse(
                "insert concrete-type sub entity; abstract-type is-abstract sub entity;"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void testMatchDataTypeQuery() {
        GetQuery expected = match(var("x").datatype(AttributeType.DataType.DOUBLE)).get();
        GetQuery parsed = parse("match $x datatype double; get;");

        assertEquals(expected, parsed);
    }

    @Test
    public void whenParsingDateKeyword_ParseAsTheCorrectDataType() {
        GetQuery expected = match(var("x").datatype(AttributeType.DataType.DATE)).get();
        GetQuery parsed = parse("match $x datatype date; get;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testInsertDataTypeQuery() {
        InsertQuery expected = insert(label("my-type").sub("resource").datatype(AttributeType.DataType.LONG));
        InsertQuery parsed = parse("insert my-type sub resource, datatype long;");
        assertEquals(expected, parsed);
    }

    @Test
    public void testEscapeString() {
        String unescaped = "This has \"double quotes\" and a single-quoted backslash: '\\'";
        String escaped = "This has \\\"double quotes\\\" and a single-quoted backslash: \\'\\\\\\'";

        InsertQuery expected = insert(var().isa("movie").has("title", unescaped));
        InsertQuery parsed = parse("insert isa movie has title '" + escaped + "';");
        assertEquals(expected, parsed);
    }

    @Test
    public void whenQueryToStringWithKeyword_EscapeKeywordWithQuotes() {
        assertEquals("match $x isa \"date\"; get $x;", match(var("x").isa("date")).get().toString());
    }

    @Test
    public void whenParsingQueryWithComments_TheyAreIgnored() {
        AggregateQuery<Value> expected = match(var("x").isa("movie")).aggregate(count());
        AggregateQuery<Value> parsed = parse(
                "match \n# there's a comment here\n$x isa###WOW HERES ANOTHER###\r\nmovie; aggregate count;"
        );
        assertEquals(expected, parsed);
    }

    @Test
    public void testInsertRules() {
        String when = "$x isa movie;";
        String then = "id '123' isa movie;";
        Pattern whenPattern = and(var("x").isa("movie"));
        Pattern thenPattern = and(var().id(ConceptId.of("123")).isa("movie"));

        InsertQuery expected = insert(
                label("my-rule-thing").sub("rule"), var().isa("my-rule-thing").when(whenPattern).then(thenPattern)
        );

        InsertQuery parsed = parse(
                "insert 'my-rule-thing' sub rule; \n" +
                        "isa my-rule-thing, when {" + when + "}, then {" + then + "};"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void testQueryParserWithoutGraph() {
        String queryString = "match $x isa movie; get $x;";
        GetQuery query = parse("match $x isa movie; get $x;");
        assertEquals(queryString, query.toString());
    }

    @Test
    public void testParseBoolean() {
        assertEquals("insert has flag true;", parse("insert has flag true;").toString());
    }

    @Test
    public void testParseAggregateGroup() {
        AggregateQuery<AnswerGroup<ConceptMap>> expected = match(var("x").isa("movie")).aggregate(group("x"));
        AggregateQuery<AnswerGroup<ConceptMap>> parsed = parse("match $x isa movie; aggregate group $x;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testParseAggregateGroupCount() {
        AggregateQuery<AnswerGroup<Value>> expected = match(var("x").isa("movie")).aggregate(group("x", count()));
        AggregateQuery<AnswerGroup<Value>> parsed = parse("match $x isa movie; aggregate group $x, count;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testParseAggregateStd() {
        AggregateQuery<Value> expected = match(var("x").isa("movie")).aggregate(std("x"));
        AggregateQuery<Value> parsed = parse("match $x isa movie; aggregate std $x;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testParseAggregateToString() {
        String query = "match $x isa movie; aggregate group $x, count;";
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
        GetQuery expected = match(var().has("title", "Godfather").has("tmdb-vote-count", var("x"))).get();
        GetQuery parsed = parse("match has title 'Godfather' has tmdb-vote-count $x; get;");
        assertEquals(expected, parsed);
    }

    @Test
    public void testRegexResourceType() {
        GetQuery expected = match(var("x").regex("(fe)?male")).get();
        GetQuery parsed = parse("match $x regex /(fe)?male/; get;");
        assertEquals(expected, parsed);
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

        Assert.assertEquals(AttributeType.DataType.BOOLEAN, property.dataType());
    }

    @Test
    public void testParseKey() {
        assertEquals("match $x key name; get $x;", parse("match $x key name; get $x;").toString());
    }

    @Test
    public void testParseListEmpty() {
        List<Query<?>> queries = Graql.parseList("").collect(toList());
        assertEquals(0, queries.size());
    }

    @Test
    public void testParseListOneMatch() {
        String getString = "match $y isa movie; get;";

        List<Query<?>> queries = Graql.parseList(getString).collect(toList());

        assertEquals(Arrays.asList(match(var("y").isa("movie")).get()), queries);
    }

    @Test
    public void testParseListOneInsert() {
        String insertString = "insert $x isa movie;";

        List<Query<?>> queries = Graql.parseList(insertString).collect(toList());

        assertEquals(Arrays.asList(insert(var("x").isa("movie"))), queries);
    }

    @Test
    public void testParseListOneInsertWithWhitespacePrefix() {
        String insertString = " insert $x isa movie;";

        List<Query<?>> queries = Graql.parseList(insertString).collect(toList());

        assertEquals(Arrays.asList(insert(var("x").isa("movie"))), queries);
    }

    @Test
    public void testParseListOneInsertWithPrefixComment() {
        String insertString = "#hola\ninsert $x isa movie;";

        List<Query<?>> queries = Graql.parseList(insertString).collect(toList());

        assertEquals(Arrays.asList(insert(var("x").isa("movie"))), queries);
    }

    @Test
    public void testParseList() {
        String insertString = "insert $x isa movie;";
        String getString = "match $y isa movie; get;";

        List<Query<?>> queries = Graql.parseList(insertString + getString).collect(toList());

        assertEquals(Arrays.asList(insert(var("x").isa("movie")), match(var("y").isa("movie")).get()), queries);
    }

    @Test
    public void testParseListMatchInsert() {
        String matchString = "match $y isa movie;";
        String insertString = "insert $x isa movie;";

        List<Query<?>> queries = Graql.parseList(matchString + insertString).collect(toList());

        assertEquals(Arrays.asList(match(var("y").isa("movie")).insert(var("x").isa("movie"))), queries);
    }

    @Test
    public void testParseMatchInsertBeforeAndAfter() {
        String matchString = "match $y isa movie; limit 1;";
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
            List<Query<?>> queries = Graql.parseList(option).collect(toList());
            assertEquals(option, 2, queries.size());
        });
    }

    @Test
    public void testParseManyMatchInsertWithoutStackOverflow() {
        int numQueries = 10_000;
        String matchInsertString = "match $x; insert $y;";
        String longQueryString = Strings.repeat(matchInsertString, numQueries);
        Query<?> matchInsert = match(var("x")).insert(var("y"));

        List<Query<?>> queries = Graql.parseList(longQueryString).collect(toList());

        assertEquals(Collections.nCopies(numQueries, matchInsert), queries);
    }

    @Test
    public void whenParsingAListOfQueriesWithASyntaxError_ReportError() {
        String queryText = "define person has name"; // note no semicolon

        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage("define person has name"); // Message should refer to line

        //noinspection ResultOfMethodCallIgnored
        Graql.parseList(queryText).collect(toList());
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
        exception.expectMessage("':'");
        //noinspection ResultOfMethodCallIgnored
        parse("match (actor $x, $y) isa has-cast; get;");
    }

    @Test
    public void testMissingComma() {
        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage("','");
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
        parse("match $x isa name; aggregate group;");
    }

    @Test
    public void whenParsingAggregateWithWrongName_Throw() {
        exception.expect(GraqlSyntaxException.class);
        //noinspection ResultOfMethodCallIgnored
        parse("match $x isa name; aggregate hello $x;");
    }

    @Test
    public void regexPredicateParsesCharacterClassesCorrectly() {
        assertEquals(match(var("x").val(regex("\\d"))).get(), parse("match $x /\\d/; get;"));
    }

    @Test
    public void regexPredicateParsesQuotesCorrectly() {
        assertEquals(match(var("x").val(regex("\""))).get(), parse("match $x /\"/; get;"));
    }

    @Test
    public void regexPredicateParsesBackslashesCorrectly() {
        assertEquals(match(var("x").val(regex("\\\\"))).get(), parse("match $x /\\\\/; get;"));
    }

    @Test
    public void regexPredicateParsesNewlineCorrectly() {
        assertEquals(match(var("x").val(regex("\\n"))).get(), parse("match $x /\\n/; get;"));
    }

    @Test
    public void regexPredicateParsesForwardSlashesCorrectly() {
        assertEquals(match(var("x").val(regex("/"))).get(), parse("match $x /\\//; get;"));
    }

    @Test
    public void whenValueEqualityToString_CreateValidQueryString() {
        Query<?> query = match(var("x").val(eq(var("y")))).get();

        assertEquals(query, Graql.parse(query.toString()));
    }

    private static void assertParseEquivalence(String query) {
        assertEquals(query, parse(query).toString());
    }
}