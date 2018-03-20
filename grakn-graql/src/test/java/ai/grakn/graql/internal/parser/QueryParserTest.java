/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.graql.internal.parser;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.QueryParser;
import ai.grakn.graql.UndefineQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.analytics.ConnectedComponentQuery;
import ai.grakn.graql.analytics.KCoreQuery;
import ai.grakn.graql.internal.pattern.property.DataTypeProperty;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.query.aggregate.AbstractAggregate;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.ask;
import static ai.grakn.graql.Graql.contains;
import static ai.grakn.graql.Graql.count;
import static ai.grakn.graql.Graql.define;
import static ai.grakn.graql.Graql.eq;
import static ai.grakn.graql.Graql.group;
import static ai.grakn.graql.Graql.gt;
import static ai.grakn.graql.Graql.gte;
import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.lt;
import static ai.grakn.graql.Graql.lte;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.neq;
import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.parse;
import static ai.grakn.graql.Graql.regex;
import static ai.grakn.graql.Graql.select;
import static ai.grakn.graql.Graql.std;
import static ai.grakn.graql.Graql.undefine;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.Graql.withoutGraph;
import static ai.grakn.graql.Order.desc;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;
import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class QueryParserTest {

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
                        "$brando val \"Marl B\" isa person;\n" +
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
                        "$t val = \"Apocalypse Now\" or {$t val < 'Juno'; $t val > 'Godfather';} or $t val 'Spy';" +
                        "$t val !='Apocalypse Now'; get;\n"
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
                        "{$t val <= 'Juno'; $t val >= 'Godfather'; $t val != 'Heat';} or $t val = 'The Muppets'; get;"
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
                        "$n val contains 'ar' or $n val /^M.*$/; get;"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void whenParsingContainsPredicateWithAVariable_ResultMatchesJavaGraql() {
        GetQuery expected = match(var("x").val(contains(var("y")))).get();

        GetQuery parsed = parse("match $x val contains $y; get;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testValueEqualsVariableQuery() {
        GetQuery expected = match(var("s1").val(var("s2"))).get();

        GetQuery parsed = parse("match $s1 val = $s2; get;");

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

    @Test
    public void testModifierQuery() {
        GetQuery expected = match(
                var("y").isa("movie").has("title", var("n"))
        ).orderBy("n").limit(4).offset(2).get();

        GetQuery parsed =
                parse("match $y isa movie, has title $n; order by $n; limit 4; offset 2; get;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testSchemaQuery() {
        GetQuery expected = match(var("x").plays("actor")).orderBy("x").get();
        GetQuery parsed = parse("match $x plays actor; order by $x asc; get;");
        assertEquals(expected, parsed);
    }

    @Test
    public void testOrderQuery() {
        GetQuery expected = match(var("x").isa("movie").has("release-date", var("r"))).orderBy("r", desc).get();
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
                        "$y val 'crime';" +
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
                "match $x isa movie; { $y isa genre val 'drama'; ($x, $y); } or $x val 'The Muppets'; get;"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void whenComparingPositiveAskQueryUsingGraqlAndJavaGraql_TheyAreEquivalent() {
        AggregateQuery<Boolean> expected = match(var("x").isa("movie").has("title", "Godfather")).aggregate(ask());
        AggregateQuery<Boolean> parsed = parse("match $x isa movie has title 'Godfather'; aggregate ask;");
        assertEquals(expected, parsed);
    }

    @Test
    public void whenComparingNegativeAskQueryUsingGraqlAndJavaGraql_TheyAreEquivalent() {
        AggregateQuery<Boolean> expected = match(var("x").isa("movie").has("title", "Dogfather")).aggregate(ask());
        AggregateQuery<Boolean> parsed = parse("match $x isa movie has title 'Dogfather'; aggregate ask;");
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
        Var x = var("x");
        Var y = var("y");

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
        AggregateQuery<Boolean> expected = match(var("x").isa("movie")).aggregate(ask());
        AggregateQuery<Boolean> parsed = parse(
                "match \n# there's a comment here\n$x isa###WOW HERES ANOTHER###\r\nmovie; aggregate ask;"
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
    public void testParseAggregate() {
        AggregateQuery<?> expected = match(var("x").isa("movie"))
                .aggregate(select(count().as("c"), group("x").as("g")));

        AggregateQuery<Map<String, Object>> parsed =
                parse("match $x isa movie; aggregate (count as c, group $x as g);");

        assertEquals(expected, parsed);
    }

    @Test
    public void testParseStdev() {
        AggregateQuery<?> expected = match(var("x").isa("movie")).aggregate(std("x"));

        AggregateQuery<Map<String, Object>> parsed =
                parse("match $x isa movie; aggregate std $x;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testParseAggregateToString() {
        String query = "match $x isa movie; aggregate group $x (count as c);";
        assertEquals(query, parse(query).toString());
    }

    @Test
    public void testCustomAggregate() {
        QueryBuilder qb = withoutGraph();

        QueryParser parser = qb.parser();

        parser.registerAggregate("get-any", args -> new GetAny((Var) args.get(0)));

        AggregateQuery<Concept> expected = qb.match(var("x").isa("movie")).aggregate(new GetAny(Graql.var("x")));
        AggregateQuery<Concept> parsed = qb.parse("match $x isa movie; aggregate get-any $x;");

        assertEquals(expected, parsed);
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
    public void testParseComputeClusterUsingCC() {
        assertParseEquivalence("compute cluster in movie, person; using connected-component;");
    }

    @Test
    public void testParseComputeClusterUsingCCWithMembers() {
        assertParseEquivalence("compute cluster in movie, person; using connected-component where members = true;");
    }

    @Test
    public void testParseComputeClusterUsingCCWithMembersThenSize() {
        ConnectedComponentQuery<?> expected = Graql.compute().cluster().usingConnectedComponent().in("movie", "person").members(true).clusterSize(10);
        ConnectedComponentQuery<?> parsed = Graql.parse(
                "compute cluster in movie, person; using connected-component where members = true size = 10;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testParseComputeClusterUsingCCWithSizeThenMembers() {
        ConnectedComponentQuery<?> expected = Graql.compute().cluster().usingConnectedComponent().in("movie", "person").clusterSize(10).members(true);
        ConnectedComponentQuery<?> parsed = Graql.parse(
                "compute cluster in movie, person; using connected-component where size = 10 members=true;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testParseComputeClusterUsingCCWithSizeThenMembersThenSize() {
        ConnectedComponentQuery<?> expected =
                Graql.compute().cluster().usingConnectedComponent().in("movie", "person").clusterSize(10).members(true).clusterSize(15);

        ConnectedComponentQuery<?> parsed = Graql.parse(
                "compute cluster in movie, person; using connected-component where size = 10 members = true size = 15;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testParseComputeClusterUsingKCore() {
        assertParseEquivalence("compute cluster in movie, person; using k-core;");
    }

    @Test
    public void testParseComputeClusterUsingKCoreWithK() {
        KCoreQuery expected = Graql.compute().cluster().usingKCore().in("movie", "person").kValue(10);
        KCoreQuery parsed = Graql.parse(
                "compute cluster in movie, person; using k-core where k = 10;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testParseComputeClusterUsingKCoreWithKTwice() {
        KCoreQuery expected = Graql.compute().cluster().usingKCore().in("movie", "person").kValue(10);
        KCoreQuery parsed = Graql.parse(
                "compute cluster in movie, person; using k-core where k = 5 k = 10;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testParseComputeDegree() {
        assertParseEquivalence("compute centrality in movie; using degree;");
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

        VarPatternAdmin var = query.match().admin().getPattern().varPatterns().iterator().next();

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
        List<Query<?>> queries = Graql.parser().parseList("").collect(toList());
        assertEquals(0, queries.size());
    }

    @Test
    public void testParseListOneMatch() {
        String getString = "match $y isa movie; limit 1; get;";

        List<Query<?>> queries = Graql.parser().parseList(getString).collect(toList());

        assertEquals(ImmutableList.of(match(var("y").isa("movie")).limit(1).get()), queries);
    }

    @Test
    public void testParseListOneInsert() {
        String insertString = "insert $x isa movie;";

        List<Query<?>> queries = Graql.parser().parseList(insertString).collect(toList());

        assertEquals(ImmutableList.of(insert(var("x").isa("movie"))), queries);
    }

    @Test
    public void testParseListOneInsertWithWhitespacePrefix() {
        String insertString = " insert $x isa movie;";

        List<Query<?>> queries = Graql.parser().parseList(insertString).collect(toList());

        assertEquals(ImmutableList.of(insert(var("x").isa("movie"))), queries);
    }

    @Test
    public void testParseListOneInsertWithPrefixComment() {
        String insertString = "#hola\ninsert $x isa movie;";

        List<Query<?>> queries = Graql.parser().parseList(insertString).collect(toList());

        assertEquals(ImmutableList.of(insert(var("x").isa("movie"))), queries);
    }

    @Test
    public void testParseList() {
        String insertString = "insert $x isa movie;";
        String getString = "match $y isa movie; limit 1; get;";

        List<Query<?>> queries = Graql.parser().parseList(insertString + getString).collect(toList());

        assertEquals(ImmutableList.of(
                insert(var("x").isa("movie")),
                match(var("y").isa("movie")).limit(1).get()
        ), queries);
    }

    @Test
    public void testParseListMatchInsert() {
        String matchString = "match $y isa movie; limit 1;";
        String insertString = "insert $x isa movie;";

        List<Query<?>> queries = Graql.parser().parseList(matchString + insertString).collect(toList());

        assertEquals(ImmutableList.of(
                match(var("y").isa("movie")).limit(1).insert(var("x").isa("movie"))
        ), queries);
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
            List<Query<?>> queries = Graql.parser().parseList(option).collect(toList());
            assertEquals(option, 2, queries.size());
        });
    }

    @Test
    public void testParseManyMatchInsertWithoutStackOverflow() {
        int numQueries = 10_000;
        String matchInsertString = "match $x; insert $y;";
        String longQueryString = Strings.repeat(matchInsertString, numQueries);
        Query<?> matchInsert = match(var("x")).insert(var("y"));

        List<Query<?>> queries = Graql.parser().parseList(longQueryString).collect(toList());

        assertEquals(Collections.nCopies(numQueries, matchInsert), queries);
    }

    @Test
    public void whenParsingAVeryLargeQuery_DontRunOutOfMemory() {
        int bigNumber = 1 << 20;
        String queryText1 = "match $x isa movie; insert ($x, $x) isa has-genre;";
        String queryText2 = "match $x isa person; insert ($x, $x) isa has-genre;";
        Query query1 = Graql.parse(queryText1);
        Query query2 = Graql.parse(queryText2);

        String massiveQuery = Strings.repeat(queryText1 + queryText2, bigNumber);

        final int[] count = {0, 0};

        Graql.parser().parseList(new StringReader(massiveQuery)).forEach(q -> {
            if (q.equals(query1)) {
                count[0]++;
            } else if (q.equals(query2)) {
                count[1]++;
            } else {
                fail("Bad query: " + q);
            }
        });

        assertEquals(bigNumber, count[0]);
        assertEquals(bigNumber, count[1]);
    }

    @Test
    public void whenParsingAnInfiniteListOfQueriesAndRetrievingFirstFewQueries_Terminate() {
        String queryText1 = "match $x isa movie; insert ($x, $x) isa has-genre;";
        String queryText2 = "match $x isa person; insert ($x, $x) isa has-genre;";
        Query query1 = Graql.parse(queryText1);
        Query query2 = Graql.parse(queryText2);

        char[] queryChars = (queryText1 + queryText2).toCharArray();

        InputStream infStream = new InputStream() {
            int pos = 0;

            @Override
            public int read() throws IOException {
                char c = queryChars[pos];
                pos += 1;
                if (pos >= queryChars.length) {
                    pos -= queryChars.length;
                }
                return c;
            }
        };

        Stream<Query<?>> queries = Graql.parser().parseList(new InputStreamReader(infStream));

        Iterator<Query<?>> iterator = queries.iterator();

        assertEquals(query1, iterator.next());
        assertEquals(query2, iterator.next());
        assertEquals(query1, iterator.next());
        assertEquals(query2, iterator.next());

        assertTrue(iterator.hasNext());
    }

    @Test
    public void whenParsingAnInfiniteListOfQueriesWithASyntaxError_Throw() {
        String queryText1 = "match $x isa movie; insert ($x, $x) isa has-genre;";
        String queryText2 = "match $x isa person insert ($x, $x) isa has-genre;";
        Query query1 = Graql.parse(queryText1);

        char[] queryChars = (queryText1 + queryText2).toCharArray();

        InputStream infStream = new InputStream() {
            int pos = 0;

            @Override
            public int read() throws IOException {
                char c = queryChars[pos];
                pos += 1;
                if (pos >= queryChars.length) {
                    pos -= queryChars.length;
                }
                return c;
            }
        };

        Stream<Query<?>> queries = Graql.parser().parseList(new InputStreamReader(infStream));

        Iterator<Query<?>> iterator = queries.iterator();

        assertEquals(query1, iterator.next());

        exception.expect(GraqlSyntaxException.class);
        iterator.next();
    }

    @Test
    public void whenParsingAListOfQueriesWithASyntaxError_ReportError() {
        String queryText = "define person has name"; // note no semicolon

        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage("define person has name"); // Message should refer to line

        //noinspection ResultOfMethodCallIgnored
        Graql.parser().parseList(queryText).collect(toList());
    }

    @Test
    public void whenParsingAQueryWithReifiedAttributeRelationshipSyntax_ItIsEquivalentToJavaGraql() {
        assertParseEquivalence("match $x has name $z via $x; get $x;");
    }

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
    public void whenParsingAggregateWithWrongArgumentNumber_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(ErrorMessage.AGGREGATE_ARGUMENT_NUM.getMessage("count", 0, 1));
        //noinspection ResultOfMethodCallIgnored
        parse("match $x isa name; aggregate count $x;");
    }

    @Test
    public void whenParsingAggregateWithWrongVariableArgumentNumber_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(ErrorMessage.AGGREGATE_ARGUMENT_NUM.getMessage("group", "1-2", 0));
        //noinspection ResultOfMethodCallIgnored
        parse("match $x isa name; aggregate group;");
    }

    @Test
    public void whenParsingAggregateWithWrongName_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(ErrorMessage.UNKNOWN_AGGREGATE.getMessage("hello"));
        //noinspection ResultOfMethodCallIgnored
        parse("match $x isa name; aggregate hello $x;");
    }

    @Test
    public void regexPredicateParsesCharacterClassesCorrectly() {
        assertEquals(match(var("x").val(regex("\\d"))).get(), parse("match $x val /\\d/; get;"));
    }

    @Test
    public void regexPredicateParsesQuotesCorrectly() {
        assertEquals(match(var("x").val(regex("\""))).get(), parse("match $x val /\"/; get;"));
    }

    @Test
    public void regexPredicateParsesBackslashesCorrectly() {
        assertEquals(match(var("x").val(regex("\\\\"))).get(), parse("match $x val /\\\\/; get;"));
    }

    @Test
    public void regexPredicateParsesNewlineCorrectly() {
        assertEquals(match(var("x").val(regex("\\n"))).get(), parse("match $x val /\\n/; get;"));
    }

    @Test
    public void regexPredicateParsesForwardSlashesCorrectly() {
        assertEquals(match(var("x").val(regex("/"))).get(), parse("match $x val /\\//; get;"));
    }

    @Test
    public void whenParsingAQueryAndDefiningAllVars_AllVarsExceptLabelsAreDefined() {
        QueryParser parser = Graql.parser();
        parser.defineAllVars(true);
        GetQuery query = parser.parseQuery("match ($x, $y) isa foo; get;");

        Conjunction<PatternAdmin> conjunction = query.match().admin().getPattern();

        Set<PatternAdmin> patterns = conjunction.getPatterns();

        VarPatternAdmin pattern = Iterables.getOnlyElement(patterns).asVarPattern();

        assertTrue(pattern.var().isUserDefinedName());

        IsaProperty property = pattern.getProperty(IsaProperty.class).get();

        assertFalse(property.type().var().isUserDefinedName());
    }

    @Test
    public void whenValueEqualityToString_CreateValidQueryString() {
        Query<?> query = match(var("x").val(eq(var("y")))).get();

        assertEquals(query, Graql.parse(query.toString()));
    }

    private static void assertParseEquivalence(String query) {
        assertEquals(query, parse(query).toString());
    }

    class GetAny extends AbstractAggregate<Answer, Concept> {

        private final Var varName;

        GetAny(Var varName) {
            this.varName = varName;
        }

        @SuppressWarnings("OptionalGetWithoutIsPresent")
        @Override
        public Concept apply(Stream<? extends Answer> stream) {
            return stream.findAny().get().get(varName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GetAny getAny = (GetAny) o;

            return varName.equals(getAny.varName);
        }

        @Override
        public int hashCode() {
            return varName.hashCode();
        }
    }
}