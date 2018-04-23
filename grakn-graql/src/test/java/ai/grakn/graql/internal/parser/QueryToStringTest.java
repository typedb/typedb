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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.parser;

import ai.grakn.concept.AttributeType;
import ai.grakn.graql.analytics.ComputeQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.test.rule.SampleKBContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.contains;
import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.lte;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.neq;
import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;

public class QueryToStringTest {

    private QueryBuilder qb;

    @ClassRule
    public static final SampleKBContext rule = MovieKB.context();

    @Before
    public void setUp() {
        qb = rule.tx().graql();
    }

    @Test
    public void testSimpleGetQueryToString() {
        assertSameResults(qb.match(var("x").isa("movie").has("title", "Godfather")).get());
    }

    @Test
    public void testComplexQueryToString() {
        GetQuery query = qb.match(
                var("x").isa("movie"),
                var().rel("x").rel("y"),
                or(
                        var("y").isa("person"),
                        var("y").isa("genre").val(neq("crime"))
                ),
                var("y").has("name", var("n"))
        ).orderBy("n").limit(8).offset(4).get("x", "y");
        assertEquivalent(query, query.toString());
    }

    @Test
    public void testQueryWithResourcesToString() {
        assertSameResults(qb.match(var("x").has("tmdb-vote-count", lte(400))).get());
    }

    @Test
    public void testQueryWithSubToString() {
        assertSameResults(qb.match(var("x").sub(var("y"))).get());
    }

    @Test
    public void testQueryWithPlaysToString() {
        assertSameResults(qb.match(var("x").plays(var("y"))).get());
    }

    @Test
    public void testQueryWithRelatesToString() {
        assertSameResults(qb.match(var("x").relates(var("y"))).get());
    }

    @Test
    public void testQueryWithDatatypeToString() {
        assertSameResults(qb.match(var("x").datatype(AttributeType.DataType.LONG)).get());
    }

    @Test
    public void testQueryIsAbstractToString() {
        assertSameResults(qb.match(var("x").isAbstract()).get());
    }

    @Test
    public void testQueryWithThenToString() {
        assertValidToString(qb.insert(var("x").isa("a-rule-type").then(and(qb.parser().parsePatterns("$x isa movie;")))));
    }

    @Test
    public void testQueryWithWhenToString() {
        assertValidToString(qb.insert(var("x").isa("a-rule-type").when(and(qb.parser().parsePatterns("$x isa movie;")))));
    }

    private void assertValidToString(InsertQuery query) {
        //No need to execute the insert query
        InsertQuery parsedQuery = qb.parse(query.toString());
        assertEquals(query.toString(), parsedQuery.toString());
    }

    @Test
    public void testInsertQueryToString() {
        assertEquals("insert $x isa movie;", qb.insert(var("x").isa("movie")).toString());
    }

    @Test
    public void testEscapeStrings() {
        assertEquals("insert $x val \"hello\\nworld\";", qb.insert(var("x").val("hello\nworld")).toString());
    }

    @Test
    public void testQuoteIds() {
        assertEquals(
                "match $a (\"hello\\tworld\");",
                match(var("a").rel(label("hello\tworld"))).toString()
        );
    }

    @Test
    public void testQuoteIdsNumbers() {
        assertEquals(
                "match $a (\"1hi\");",
                match(var("a").rel(label("1hi"))).toString()
        );
    }

    @Test
    public void testHas() {
        assertEquals("insert $x has thingy;", qb.insert(var("x").has("thingy")).toString());
    }

    @Test
    public void testComputeQueryToString() {
        assertEquals("compute count;", qb.compute().count().toString());
    }

    @Test
    public void testComputeQuerySubgraphToString() {
        ComputeQuery query = qb.compute().centrality().usingDegree().in("movie", "person");
        assertEquivalent(query, "compute centrality in movie, person; using degree;");
    }

    @Test
    public void testClusterToString() {
        ComputeQuery query = qb.compute().cluster().usingConnectedComponent().in("movie", "person");
        assertEquivalent(query, "compute cluster in movie, person; using connected-component;");

        query = qb.compute().cluster().usingKCore().in("movie", "person");
        assertEquivalent(query, "compute cluster in movie, person; using k-core;");
    }

    @Test
    public void testCCSizeToString() {
        ComputeQuery query = qb.compute().cluster().usingConnectedComponent().in("movie", "person").size(10);
        assertEquivalent(query, "compute cluster in movie, person; using connected-component where size = 10;");
    }

    @Test
    public void testKCoreToString() {
        ComputeQuery query = qb.compute().cluster().usingKCore().in("movie", "person").k(10);
        assertEquivalent(query, "compute cluster in movie, person; using k-core where k = 10;");
    }

    @Test
    public void testCentralityOf() {
        ComputeQuery query = qb.compute().centrality().usingDegree().in("movie", "person").of("person");
        assertEquivalent(query, "compute centrality of person in movie, person; using degree;");

        query = qb.compute().centrality().usingKCore().in("movie", "person").of("person").minK(5);
        assertEquivalent(query, "compute centrality of person in movie, person; using k-core where min-k = 5;");
    }

    @Test
    public void testQueryToStringWithReservedKeywords() {
        GetQuery query = qb.match(var("x").isa("isa")).get();
        assertEquals("match $x isa \"isa\"; get $x;", query.toString());
    }

    @Test
    public void testRepeatRoleplayerToString() {
        assertEquals("match ($x, $x);", match(var().rel("x").rel("x")).toString());
    }

    @Test
    public void testMatchInsertToString() {
        InsertQuery query = qb.match(var("x").isa("movie")).insert(var("x").has("title", "hello"));
        assertEquals("match $x isa movie;\ninsert $x has title \"hello\";", query.toString());
    }

    @Test
    public void testZeroToString() {
        assertEquals("match $x val 0.0;", qb.match(var("x").val(0.0)).toString());
    }

    @Test
    public void testExponentsToString() {
        assertEquals("match $x val 1000000000.0;", qb.match(var("x").val(1_000_000_000.0)).toString());
    }

    @Test
    public void testDecimalToString() {
        assertEquals("match $x val 0.0001;", qb.match(var("x").val(0.0001)).toString());
    }

    @Test
    public void whenCallingToStringOnDeleteQuery_ItLooksLikeOriginalQuery() {
        String query = "match $x isa movie; delete $x;";

        assertEquals(query, qb.parse(query).toString());
    }

    @Test
    public void whenCallingToStringOnAQueryWithAContainsPredicate_ResultIsCorrect() {
        Match match = match(var("x").val(contains(var("y"))));

        assertEquals("match $x val contains $y;", match.toString());
    }

    private void assertSameResults(GetQuery query) {
        assertEquals(query.execute(), qb.parse(query.toString()).execute());
    }

    private void assertEquivalent(Query<?> query, String queryString) {
        assertEquals(queryString, query.toString());
        assertEquals(query.toString(), qb.parse(queryString).toString());
    }
}
