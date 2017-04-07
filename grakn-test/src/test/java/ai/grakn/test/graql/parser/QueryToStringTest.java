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

import ai.grakn.concept.ResourceType;
import ai.grakn.graphs.MovieGraph;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.GraphContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.graql.Graql.and;
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
    public static final GraphContext rule = GraphContext.preLoad(MovieGraph.get());

    @Before
    public void setUp() {
        qb = rule.graph().graql();
    }

    @Test
    public void testSimpleMatchQueryToString() {
        assertSameResults(qb.match(var("x").isa("movie").label("Godfather")));
    }

    @Test
    public void testComplexQueryToString() {
        MatchQuery query = qb.match(
                var("x").isa("movie"),
                var().rel("x").rel("y"),
                or(
                        var("y").isa("person"),
                        var("y").isa("genre").val(neq("crime"))
                ),
                var("y").has("name", var("n"))
        ).orderBy("n").select("x", "y").limit(8).offset(4);
        assertSameResults(query);
    }

    @Test
    public void testQueryWithResourcesToString() {
        assertSameResults(qb.match(var("x").has("tmdb-vote-count", lte(400))));
    }

    @Test
    public void testQueryWithSubToString() {
        assertSameResults(qb.match(var("x").sub(var("y"))));
    }

    @Test
    public void testQueryWithPlaysToString() {
        assertSameResults(qb.match(var("x").plays(var("y"))));
    }

    @Test
    public void testQueryWithRelatesToString() {
        assertSameResults(qb.match(var("x").relates(var("y"))));
    }

    @Test
    public void testQueryWithHasScopeToString() {
        assertEquals("match $x has-scope $y;", qb.match(var("x").hasScope(var("y"))).toString());
    }

    @Test
    public void testQueryWithDatatypeToString() {
        assertSameResults(qb.match(var("x").datatype(ResourceType.DataType.LONG)));
    }

    @Test
    public void testQueryIsAbstractToString() {
        assertSameResults(qb.match(var("x").isAbstract()));
    }

    @Test
    public void testQueryWithRhsToString() {
        assertValidToString(qb.insert(var("x").isa("a-rule-type").rhs(and(qb.parsePatterns("$x isa movie;")))));
    }

    @Test
    public void testQueryWithLhsToString() {
        assertValidToString(qb.insert(var("x").isa("a-rule-type").lhs(and(qb.parsePatterns("$x isa movie;")))));
    }

    private void assertValidToString(InsertQuery query){
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
        ComputeQuery query = qb.compute().degree().in("movie", "person");
        assertEquivalent(query, "compute degrees in movie, person;");
    }

    @Test
    public void testClusterToString() {
        ComputeQuery query = qb.compute().cluster().in("movie", "person");
        assertEquivalent(query, "compute cluster in movie, person;");
    }

    @Test
    public void testClusterSizeToString() {
        ComputeQuery query = qb.compute().cluster().in("movie", "person").clusterSize(10);
        assertEquivalent(query, "compute cluster in movie, person; size 10;");
    }

    @Test
    public void testDegreeOf() {
        ComputeQuery query = qb.compute().degree().in("movie", "person").of("person");
        assertEquivalent(query, "compute degrees of person in movie, person;");
    }

    @Test
    public void testQueryToStringWithReservedKeywords() {
        MatchQuery query = qb.match(var("x").isa("isa"));
        assertEquals("match $x isa \"isa\";", query.toString());
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

    @Test(expected=UnsupportedOperationException.class)
    public void testToStringUnsupported() {
        //noinspection ResultOfMethodCallIgnored
        qb.match(var("x").isa(var().val("abc"))).toString();
    }

    private void assertSameResults(MatchQuery query) {
        assertEquals(query.execute(), qb.parse(query.toString()).execute());
    }

    private void assertEquivalent(Query<?> query, String queryString) {
        assertEquals(queryString, query.toString());
        assertEquals(query.toString(), qb.parse(queryString).toString());
    }
}
