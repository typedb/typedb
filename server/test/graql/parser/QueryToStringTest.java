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

import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.query.ComputeQuery;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.InsertQuery;
import grakn.core.graql.query.Match;
import grakn.core.graql.query.Query;
import grakn.core.graql.query.QueryBuilder;
import org.junit.Before;
import org.junit.Test;

import static grakn.core.graql.query.pattern.Pattern.and;
import static grakn.core.graql.query.Graql.contains;
import static grakn.core.graql.query.pattern.Pattern.label;
import static grakn.core.graql.query.Graql.lte;
import static grakn.core.graql.query.Graql.match;
import static grakn.core.graql.query.Graql.neq;
import static grakn.core.graql.query.pattern.Pattern.or;
import static grakn.core.graql.query.pattern.Pattern.var;
import static grakn.core.graql.query.ComputeQuery.Algorithm.CONNECTED_COMPONENT;
import static grakn.core.graql.query.ComputeQuery.Algorithm.DEGREE;
import static grakn.core.graql.query.ComputeQuery.Algorithm.K_CORE;
import static grakn.core.graql.query.ComputeQuery.Argument.k;
import static grakn.core.graql.query.ComputeQuery.Argument.min_k;
import static grakn.core.graql.query.ComputeQuery.Argument.size;
import static grakn.core.graql.query.ComputeQuery.Method.CENTRALITY;
import static grakn.core.graql.query.ComputeQuery.Method.CLUSTER;
import static grakn.core.graql.query.ComputeQuery.Method.COUNT;
import static org.junit.Assert.assertEquals;

public class QueryToStringTest {

    private QueryBuilder qb;

    @Before
    public void setUp() {
        qb = new QueryBuilder();
    }

    @Test
    public void testSimpleGetQueryToString() {
        assertSameStringRepresentation(qb.match(var("x").isa("movie").has("title", "Godfather")).get());
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
        ).get("x", "y");
        //TODO: re-add .orderBy("n").limit(8).offset(4)
        assertEquivalent(query, query.toString());
    }

    @Test
    public void testQueryWithResourcesToString() {
        assertSameStringRepresentation(qb.match(var("x").has("tmdb-vote-count", lte(400))).get());
    }

    @Test
    public void testQueryWithSubToString() {
        assertSameStringRepresentation(qb.match(var("x").sub(var("y"))).get());
    }

    @Test
    public void testQueryWithPlaysToString() {
        assertSameStringRepresentation(qb.match(var("x").plays(var("y"))).get());
    }

    @Test
    public void testQueryWithRelatesToString() {
        assertSameStringRepresentation(qb.match(var("x").relates(var("y"))).get());
    }

    @Test
    public void testQueryWithDatatypeToString() {
        assertSameStringRepresentation(qb.match(var("x").datatype(AttributeType.DataType.LONG)).get());
    }

    @Test
    public void testQueryIsAbstractToString() {
        assertSameStringRepresentation(qb.match(var("x").isAbstract()).get());
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
        assertEquals("insert $x \"hello\\nworld\";", qb.insert(var("x").val("hello\nworld")).toString());
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
        assertEquals("compute count;", qb.compute(COUNT).toString());
    }

    @Test
    public void testComputeQuerySubgraphToString() {
        ComputeQuery query = qb.compute(CENTRALITY).using(DEGREE).in("movie", "person");
        assertEquivalent(query, "compute centrality in [movie, person], using degree;");
    }

    @Test
    public void testClusterToString() {
        ComputeQuery connectedcomponent = qb.compute(CLUSTER).using(CONNECTED_COMPONENT).in("movie", "person");
        assertEquivalent(connectedcomponent, "compute cluster in [movie, person], using connected-component;");

        ComputeQuery kcore = qb.compute(CLUSTER).using(K_CORE).in("movie", "person");
        assertEquivalent(kcore, "compute cluster in [movie, person], using k-core;");
    }

    @Test
    public void testCCSizeToString() {
        ComputeQuery query = qb.compute(CLUSTER).using(CONNECTED_COMPONENT).in("movie", "person").where(size(10));
        assertEquivalent(query, "compute cluster in [movie, person], using connected-component, where size=10;");
    }

    @Test
    public void testKCoreToString() {
        ComputeQuery query = qb.compute(CLUSTER).using(K_CORE).in("movie", "person").where(k(10));
        assertEquivalent(query, "compute cluster in [movie, person], using k-core, where k=10;");
    }

    @Test
    public void testCentralityOf() {
        ComputeQuery query = qb.compute(CENTRALITY).using(DEGREE).in("movie", "person").of("person");
        assertEquivalent(query, "compute centrality of person, in [movie, person], using degree;");

        query = qb.compute(CENTRALITY).using(K_CORE).in("movie", "person").of("person").where(min_k(5));
        assertEquivalent(query, "compute centrality of person, in [movie, person], using k-core, where min-k=5;");
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
        assertEquals("match $x 0.0;", qb.match(var("x").val(0.0)).toString());
    }

    @Test
    public void testExponentsToString() {
        assertEquals("match $x 1000000000.0;", qb.match(var("x").val(1_000_000_000.0)).toString());
    }

    @Test
    public void testDecimalToString() {
        assertEquals("match $x 0.0001;", qb.match(var("x").val(0.0001)).toString());
    }

    @Test
    public void whenCallingToStringOnDeleteQuery_ItLooksLikeOriginalQuery() {
        String query = "match $x isa movie; delete $x;";

        assertEquals(query, qb.parse(query).toString());
    }

    @Test
    public void whenCallingToStringOnAQueryWithAContainsPredicate_ResultIsCorrect() {
        Match match = match(var("x").val(contains(var("y"))));

        assertEquals("match $x contains $y;", match.toString());
    }

    private void assertSameStringRepresentation(GetQuery query) {
        assertEquals(query.toString(), qb.parse(query.toString()).toString());
    }

    private void assertEquivalent(Query<?> query, String queryString) {
        assertEquals(queryString, query.toString());
        assertEquals(query.toString(), qb.parse(queryString).toString());
    }
}