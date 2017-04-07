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
 *
 */

package ai.grakn.graql.internal.parser;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ResourceType;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.AskQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.DataTypeProperty;
import ai.grakn.graql.internal.query.aggregate.AbstractAggregate;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.contains;
import static ai.grakn.graql.Graql.count;
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
import static ai.grakn.graql.Graql.parseList;
import static ai.grakn.graql.Graql.parsePatterns;
import static ai.grakn.graql.Graql.regex;
import static ai.grakn.graql.Graql.select;
import static ai.grakn.graql.Graql.std;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.Graql.withoutGraph;
import static ai.grakn.graql.Order.desc;
import static com.google.common.collect.Lists.newArrayList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryParserTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testSimpleQuery() {
        assertEquals(
                match(var("x").isa("movie")),
                parse("match $x isa movie;")
        );
    }

    @Test
    public void testRelationQuery() {
        MatchQuery expected = match(
                var("brando").val("Marl B").isa("person"),
                var().rel("actor", "brando").rel("char").rel("production-with-cast", "prod")
        ).select("char", "prod");

        MatchQuery parsed = parse(
                "match\n" +
                        "$brando val \"Marl B\" isa person;\n" +
                        "(actor: $brando, $char, production-with-cast: $prod);\n" +
                        "select $char, $prod;"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void testPredicateQuery1() {
        MatchQuery expected = match(
                var("x").isa("movie").has("title", var("t")),
                or(
                        or(
                                var("t").val(eq("Apocalypse Now")),
                                and(var("t").val(lt("Juno")), var("t").val(gt("Godfather")))
                        ),
                        var("t").val(eq("Spy"))
                ),
                var("t").val(neq("Apocalypse Now"))
        );

        MatchQuery parsed = parse(
                "match\n" +
                "$x isa movie, has title $t;\n" +
                "$t val = \"Apocalypse Now\" or {$t val < 'Juno'; $t val > 'Godfather';} or $t val 'Spy';" +
                "$t val !='Apocalypse Now';\n"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void testPredicateQuery2() {
        MatchQuery expected = match(
                var("x").isa("movie").has("title", var("t")),
                or(
                    and(var("t").val(lte("Juno")), var("t").val(gte("Godfather")), var("t").val(neq("Heat"))),
                    var("t").val("The Muppets")
                )
        );

        MatchQuery parsed = parse(
                "match $x isa movie, has title $t;" +
                "{$t val <= 'Juno'; $t val >= 'Godfather'; $t val != 'Heat';} or $t val = 'The Muppets';"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void testPredicateQuery3() {
        MatchQuery expected = match(
                var().rel("x").rel("y"),
                var("y").isa("person").has("name", var("n")),
                or(var("n").val(contains("ar")), var("n").val(regex("^M.*$")))
        );

        MatchQuery parsed = parse(
                "match ($x, $y); $y isa person, has name $n;" +
                "$n val contains 'ar' or $n val /^M.*$/;"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void testValueEqualsVariableQuery() {
        MatchQuery expected = match(var("s1").val(var("s2")));

        MatchQuery parsed = parse("match $s1 val = $s2;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testMoviesReleasedAfterOrAtTheSameTimeAsSpy() {
        MatchQuery expected = match(
                var("x").has("release-date", gte(var("r"))),
                var().has("title", "Spy").has("release-date", var("r"))
        );

        MatchQuery parsed = parse("match $x has release-date >= $r; has title 'Spy', has release-date $r;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testTypesQuery() throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

        long date = dateFormat.parse("Mon Mar 03 00:00:00 BST 1986").getTime();

        MatchQuery expected = match(
                var("x")
                        .has("release-date", lt(date))
                        .has("tmdb-vote-count", 100)
                        .has("tmdb-vote-average", lte(9.0))
        );

        MatchQuery parsed = parse(
                "match $x has release-date < " + date + ", has tmdb-vote-count 100 has tmdb-vote-average<=9.0;"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void testLongComparatorQuery() throws ParseException {
        MatchQuery expected = match(
                var("x").isa("movie").has("tmdb-vote-count", lte(400))
        );

        MatchQuery parsed = parse("match $x isa movie, has tmdb-vote-count <= 400;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testModifierQuery() {
        MatchQuery expected = match(
                var("y").isa("movie").has("title", var("n"))
        ).orderBy("n").limit(4).offset(2).distinct();

        MatchQuery parsed =
                parse("match $y isa movie, has title $n; order by $n; limit 4; offset 2; distinct;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testOntologyQuery() {
        MatchQuery expected = match(var("x").plays("actor")).orderBy("x");
        MatchQuery parsed = parse("match $x plays actor; order by $x asc;");
        assertEquals(expected, parsed);
    }

    @Test
    public void testOrderQuery() {
        MatchQuery expected = match(var("x").isa("movie").has("release-date", var("r"))).orderBy("r", desc);
        MatchQuery parsed = parse("match $x isa movie, has release-date $r; order by $r desc;");
        assertEquals(expected, parsed);
    }

    @Test
    public void testVariablesEverywhereQuery() {
        MatchQuery expected = match(
                var().rel(var("p"), "x").rel("y"),
                var("x").isa(var("z")),
                var("y").val("crime"),
                var("z").sub("production"),
                label("has-genre").relates(var("p"))
        );

        MatchQuery parsed = parse(
                "match" +
                        "($p: $x, $y);" +
                        "$x isa $z;" +
                        "$y val 'crime';" +
                        "$z sub production;" +
                        "has-genre relates $p;"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void testOrQuery() {
        MatchQuery expected = match(
                var("x").isa("movie"),
                or(
                        and(var("y").isa("genre").val("drama"), var().rel("x").rel("y")),
                        var("x").val("The Muppets")
                )
        );

        MatchQuery parsed = parse(
                "match $x isa movie; { $y isa genre val 'drama'; ($x, $y); } or $x val 'The Muppets';"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void testPositiveAskQuery() {
        AskQuery expected = match(var("x").isa("movie").has("title", "Godfather")).ask();
        AskQuery parsed = parse("match $x isa movie has title 'Godfather'; ask;");
        assertEquals(expected, parsed);
    }

    @Test
    public void testNegativeAskQuery() {
        AskQuery expected = match(var("x").isa("movie").has("title", "Dogfather")).ask();
        AskQuery parsed = parse("match $x isa movie has title 'Dogfather'; ask;");
        assertEquals(expected, parsed);
    }

    @Test
    public void testInsertQuery() {
        InsertQuery expected = insert(var().isa("movie").has("title", "The Title"));
        InsertQuery parsed = parse("insert isa movie has title 'The Title';");
        assertEquals(expected, parsed);
    }

    @Test
    public void testDeleteQuery() {
        DeleteQuery expected = match(var("x").isa("movie").has("title", "The Title")).delete("x");
        DeleteQuery parsed = parse("match $x isa movie has title 'The Title'; delete $x;");
        assertEquals(expected, parsed);
    }

    @Test
    public void testInsertOntologyQuery() {
        InsertQuery expected = insert(
                label("pokemon").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                label("evolution").sub(Schema.MetaSchema.RELATION.getLabel().getValue()),
                label("evolves-from").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("evolves-to").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("evolution").relates("evolves-from").relates("evolves-to"),
                label("pokemon").plays("evolves-from").plays("evolves-to").has("name"),
                var("x").has("name", "Pichu").isa("pokemon"),
                var("y").has("name", "Pikachu").isa("pokemon"),
                var("z").has("name", "Raichu").isa("pokemon"),
                var().rel("evolves-from", "x").rel("evolves-to", "y").isa("evolution"),
                var().rel("evolves-from", "y").rel("evolves-to", "z").isa("evolution")
        );

        InsertQuery parsed = parse("insert " +
                "'pokemon' sub entity;" +
                "evolution sub relation;" +
                "evolves-from sub role;" +
                "label \"evolves-to\" sub role;" +
                "evolution relates evolves-from, relates evolves-to;" +
                "pokemon plays evolves-from plays evolves-to has name;" +
                "$x has name 'Pichu' isa pokemon;" +
                "$y has name 'Pikachu' isa pokemon;" +
                "$z has name 'Raichu' isa pokemon;" +
                "(evolves-from: $x ,evolves-to: $y) isa evolution;" +
                "(evolves-from: $y, evolves-to: $z) isa evolution;"
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
        MatchQuery expected = match(var("x").datatype(ResourceType.DataType.DOUBLE));
        MatchQuery parsed = parse("match $x datatype double;");

        assertEquals(expected, parsed);
    }

    @Test
    public void testInsertDataTypeQuery() {
        InsertQuery expected = insert(label("my-type").sub("resource").datatype(ResourceType.DataType.LONG));
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
    public void testComments() {
        AskQuery expected = match(var("x").isa("movie")).ask();
        AskQuery parsed = parse(
                "match \n# there's a comment here\n$x isa###WOW HERES ANOTHER###\r\nmovie; ask;"
        );
        assertEquals(expected, parsed);
    }

    @Test
    public void testInsertRules() {
        String lhs = "$x isa movie;";
        String rhs = "id '123' isa movie;";
        Pattern lhsPattern = and(parsePatterns(lhs));
        Pattern rhsPattern = and(parsePatterns(rhs));

        InsertQuery expected = insert(
                label("my-rule-thing").sub("rule"), var().isa("my-rule-thing").lhs(lhsPattern).rhs(rhsPattern)
        );

        InsertQuery parsed = parse(
                "insert 'my-rule-thing' sub rule; \n" +
                "isa my-rule-thing, lhs {" + lhs + "}, rhs {" + rhs + "};"
        );

        assertEquals(expected, parsed);
    }

    @Test
    public void testQueryParserWithoutGraph() {
        String queryString = "match $x isa movie; select $x;";
        MatchQuery query = parse("match $x isa movie; select $x;");
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

        qb.registerAggregate("get-any", args -> new GetAny((VarName) args.get(0)));

        AggregateQuery<Concept> expected = qb.match(var("x").isa("movie")).aggregate(new GetAny(VarName.of("x")));
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
    public void testParseComputeCluster() {
        assertParseEquivalence("compute cluster in movie, person; members;");
    }

    @Test
    public void testParseComputeDegree() {
        assertParseEquivalence("compute degrees in movie;");
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
    public void whenParseIncorrectSyntax_ThrowIllegalArgumentExceptionWithHelpfulError() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(allOf(
                containsString("syntax error"), containsString("line 1"),
                containsString("\nmatch $x isa "),
                containsString("\n             ^")
        ));
        parse("match $x isa ");
    }

    @Test
    public void whenParseIncorrectSyntax_ErrorMessageShouldRetainWhitespace() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(not(containsString("match$xisa")));
        parse("match $x isa ");
    }

    @Test
    public void testSyntaxErrorPointer() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(allOf(
                containsString("\nmatch $x is"),
                containsString("\n         ^")
        ));
        parse("match $x is");
    }

    @Test
    public void testHasVariable() {
        MatchQuery expected = match(var().has("title", "Godfather").has("tmdb-vote-count", var("x")));
        MatchQuery parsed = parse("match has title 'Godfather' has tmdb-vote-count $x;");
        assertEquals(expected, parsed);
    }

    @Test
    public void testRegexResourceType() {
        MatchQuery expected = match(var("x").regex("(fe)?male"));
        MatchQuery parsed = parse("match $x regex /(fe)?male/;");
        assertEquals(expected, parsed);
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

        Assert.assertEquals(ResourceType.DataType.BOOLEAN, property.getDataType());
    }

    @Test
    public void testParseHasScope() {
        assertEquals("match $x has-scope $y;", parse("match $x has-scope $y;").toString());
    }

    @Test
    public void testParseKey() {
        assertEquals("match $x key name;", parse("match $x key name;").toString());
    }

    @Test
    public void testParseListEmpty() {
        List<Query<?>> queries = parseList("");
        assertEquals(0, queries.size());
    }

    @Test
    public void testParseListOneMatch() {
        String matchString = "match $y isa movie; limit 1;";

        List<Query<?>> queries = parseList(matchString);

        assertEquals(ImmutableList.of(match(var("y").isa("movie")).limit(1)), queries);
    }

    @Test
    public void testParseListOneInsert() {
        String insertString = "insert $x isa movie;";

        List<Query<?>> queries = parseList(insertString);

        assertEquals(ImmutableList.of(insert(var("x").isa("movie"))), queries);
    }

    @Test
    public void testParseList() {
        String insertString = "insert $x isa movie;";
        String matchString = "match $y isa movie; limit 1;";

        List<Query<?>> queries = parseList(insertString + matchString);

        assertEquals(ImmutableList.of(
                insert(var("x").isa("movie")),
                match(var("y").isa("movie")).limit(1)
        ), queries);
    }

    @Test
    public void testParseListMatchInsert() {
        String matchString = "match $y isa movie; limit 1;";
        String insertString = "insert $x isa movie;";

        List<Query<?>> queries = parseList(matchString + insertString);

        assertEquals(ImmutableList.of(
                match(var("y").isa("movie")).limit(1).insert(var("x").isa("movie"))
        ), queries);
    }

    @Test
    public void testParseMatchInsertBeforeAndAfter() {
        String matchString = "match $y isa movie; limit 1;";
        String insertString = "insert $x isa movie;";
        String matchInsert = matchString + insertString;

        List<String> options = newArrayList(
                matchString + matchInsert,
                insertString + matchInsert,
                matchInsert + matchString,
                matchInsert + insertString
        );

        options.forEach(option -> {
            List<Query<?>> queries = parseList(option);
            assertEquals(option, 2, queries.size());
        });
    }

    @Test
    public void testParseManyMatchInsertWithoutStackOverflow() {
        int numQueries = 10_000;
        String matchInsertString = "match $x; insert $y;";
        String longQueryString = Strings.repeat(matchInsertString, numQueries);
        Query<?> matchInsert = match(var("x")).insert(var("y"));

        List<Query<?>> queries = parseList(longQueryString);

        assertEquals(Collections.nCopies(numQueries, matchInsert), queries);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMultipleQueriesThrowsIllegalArgumentException() {
        parse("insert $x isa movie; insert $y isa movie");
    }

    @Test
    public void testMissingColon() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("':'");
        parse("match (actor $x, $y) isa has-cast;");
    }

    @Test
    public void testMissingComma() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("','");
        parse("match ($x $y) isa has-cast;");
    }

    @Test
    public void testLimitMistake() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("limit1");
        parse("match ($x, $y); limit1;");
    }

    @Test
    public void whenParsingAggregateWithWrongArgumentNumber_Throw() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(ErrorMessage.AGGREGATE_ARGUMENT_NUM.getMessage("count", 0, 1));
        parse("match $x isa name; aggregate count $x;");
    }

    @Test
    public void whenParsingAggregateWithWrongVariableArgumentNumber_Throw() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(ErrorMessage.AGGREGATE_ARGUMENT_NUM.getMessage("group", "1-2", 0));
        parse("match $x isa name; aggregate group;");
    }

    @Test
    public void whenParsingAggregateWithWrongName_Throw() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(ErrorMessage.UNKNOWN_AGGREGATE.getMessage("hello"));
        parse("match $x isa name; aggregate hello $x;");
    }

    public static void assertQueriesEqual(MatchQuery query, MatchQuery parsedQuery) {
        assertEquals(Sets.newHashSet(query), Sets.newHashSet(parsedQuery));
    }

    private static void assertParseEquivalence(String query) {
        assertEquals(query, parse(query).toString());
    }

    class GetAny extends AbstractAggregate<Map<VarName, Concept>, Concept> {

        private final VarName varName;

        GetAny(VarName varName) {
            this.varName = varName;
        }

        @SuppressWarnings("OptionalGetWithoutIsPresent")
        @Override
        public Concept apply(Stream<? extends Map<VarName, Concept>> stream) {
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