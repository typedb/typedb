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

import grakn.core.graql.query.ComputeQuery;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.InsertQuery;
import grakn.core.graql.query.MatchClause;
import grakn.core.graql.query.Query;
import org.junit.Test;

import static grakn.core.graql.query.ComputeQuery.Algorithm.CONNECTED_COMPONENT;
import static grakn.core.graql.query.ComputeQuery.Algorithm.DEGREE;
import static grakn.core.graql.query.ComputeQuery.Algorithm.K_CORE;
import static grakn.core.graql.query.ComputeQuery.Argument.k;
import static grakn.core.graql.query.ComputeQuery.Argument.min_k;
import static grakn.core.graql.query.ComputeQuery.Argument.size;
import static grakn.core.graql.query.ComputeQuery.Method.CENTRALITY;
import static grakn.core.graql.query.ComputeQuery.Method.CLUSTER;
import static grakn.core.graql.query.ComputeQuery.Method.COUNT;
import static grakn.core.graql.query.Graql.contains;
import static grakn.core.graql.query.Graql.lte;
import static grakn.core.graql.query.Graql.match;
import static grakn.core.graql.query.Graql.neq;
import static grakn.core.graql.query.Graql.and;
import static grakn.core.graql.query.Graql.type;
import static grakn.core.graql.query.Graql.or;
import static grakn.core.graql.query.Graql.var;
import static org.junit.Assert.assertEquals;

public class QueryToStringTest {

    @Test
    public void testSimpleGetQueryToString() {
        assertSameStringRepresentation(Graql.match(var("x").isa("movie").has("title", "Godfather")).get());
    }

    @Test
    public void testComplexQueryToString() {
        GetQuery query = Graql.match(
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
        assertSameStringRepresentation(Graql.match(var("x").has("tmdb-vote-count", lte(400))).get());
    }

    @Test
    public void testQueryWithSubToString() {
        assertSameStringRepresentation(Graql.match(var("x").sub(var("y"))).get());
    }

    @Test
    public void testQueryWithPlaysToString() {
        assertSameStringRepresentation(Graql.match(var("x").plays(var("y"))).get());
    }

    @Test
    public void testQueryWithRelatesToString() {
        assertSameStringRepresentation(Graql.match(var("x").relates(var("y"))).get());
    }

    @Test
    public void testQueryWithDatatypeToString() {
        assertSameStringRepresentation(Graql.match(var("x").datatype(Query.DataType.LONG)).get());
    }

    @Test
    public void testQueryIsAbstractToString() {
        assertSameStringRepresentation(Graql.match(var("x").isAbstract()).get());
    }

    @Test
    public void testQueryWithThenToString() {
        assertValidToString(Graql.insert(var("x").isa("a-rule-type").then(and(Graql.parsePatternList("$x isa movie;")))));
    }

    @Test
    public void testQueryWithWhenToString() {
        assertValidToString(Graql.insert(var("x").isa("a-rule-type").when(and(Graql.parsePatternList("$x isa movie;")))));
    }

    private void assertValidToString(InsertQuery query) {
        //No need to execute the insert query
        InsertQuery parsedQuery = Graql.parse(query.toString());
        assertEquals(query.toString(), parsedQuery.toString());
    }

    @Test
    public void testInsertQueryToString() {
        assertEquals("insert $x isa movie;", Graql.insert(var("x").isa("movie")).toString());
    }

    @Test
    public void testEscapeStrings() {
        assertEquals("insert $x \"hello\\nworld\";", Graql.insert(var("x").val("hello\nworld")).toString());
    }

    @Test
    public void testQuoteIds() {
        assertEquals(
                "match $a (\"hello\\tworld\");",
                match(var("a").rel(type("hello\tworld"))).toString()
        );
    }

    @Test
    public void testQuoteIdsNumbers() {
        assertEquals(
                "match $a (\"1hi\");",
                match(var("a").rel(type("1hi"))).toString()
        );
    }

    @Test
    public void testHas() {
        assertEquals("insert $x has thingy;", Graql.insert(var("x").has("thingy")).toString());
    }

    @Test
    public void testComputeQueryToString() {
        assertEquals("compute count;", Graql.compute(COUNT).toString());
    }

    @Test
    public void testComputeQuerySubgraphToString() {
        ComputeQuery query = Graql.compute(CENTRALITY).using(DEGREE).in("movie", "person");
        assertEquivalent(query, "compute centrality in [movie, person], using degree;");
    }

    @Test
    public void testClusterToString() {
        ComputeQuery connectedcomponent = Graql.compute(CLUSTER).using(CONNECTED_COMPONENT).in("movie", "person");
        assertEquivalent(connectedcomponent, "compute cluster in [movie, person], using connected-component;");

        ComputeQuery kcore = Graql.compute(CLUSTER).using(K_CORE).in("movie", "person");
        assertEquivalent(kcore, "compute cluster in [movie, person], using k-core;");
    }

    @Test
    public void testCCSizeToString() {
        ComputeQuery query = Graql.compute(CLUSTER).using(CONNECTED_COMPONENT).in("movie", "person").where(size(10));
        assertEquivalent(query, "compute cluster in [movie, person], using connected-component, where size=10;");
    }

    @Test
    public void testKCoreToString() {
        ComputeQuery query = Graql.compute(CLUSTER).using(K_CORE).in("movie", "person").where(k(10));
        assertEquivalent(query, "compute cluster in [movie, person], using k-core, where k=10;");
    }

    @Test
    public void testCentralityOf() {
        ComputeQuery query = Graql.compute(CENTRALITY).using(DEGREE).in("movie", "person").of("person");
        assertEquivalent(query, "compute centrality of person, in [movie, person], using degree;");

        query = Graql.compute(CENTRALITY).using(K_CORE).in("movie", "person").of("person").where(min_k(5));
        assertEquivalent(query, "compute centrality of person, in [movie, person], using k-core, where min-k=5;");
    }

    @Test
    public void testQueryToStringWithReservedKeywords() {
        GetQuery query = Graql.match(var("x").isa("isa")).get("x");
        assertEquals("match $x isa \"isa\"; get $x;", query.toString());
    }

    @Test
    public void testRepeatRoleplayerToString() {
        assertEquals("match ($x, $x);", match(var().rel("x").rel("x")).toString());
    }

    @Test
    public void testMatchInsertToString() {
        InsertQuery query = Graql.match(var("x").isa("movie")).insert(var("x").has("title", "hello"));
        assertEquals("match $x isa movie;\ninsert $x has title \"hello\";", query.toString());
    }

    @Test
    public void testZeroToString() {
        assertEquals("match $x 0.0;", Graql.match(var("x").val(0.0)).toString());
    }

    @Test
    public void testExponentsToString() {
        assertEquals("match $x 1000000000.0;", Graql.match(var("x").val(1_000_000_000.0)).toString());
    }

    @Test
    public void testDecimalToString() {
        assertEquals("match $x 0.0001;", Graql.match(var("x").val(0.0001)).toString());
    }

    @Test
    public void whenCallingToStringOnDeleteQuery_ItLooksLikeOriginalQuery() {
        String query = "match $x isa movie; delete $x;";

        assertEquals(query, Graql.parse(query).toString());
    }

    @Test
    public void whenCallingToStringOnAQueryWithAContainsPredicate_ResultIsCorrect() {
        MatchClause match = match(var("x").val(contains(var("y"))));

        assertEquals("match $x contains $y;", match.toString());
    }

    private void assertSameStringRepresentation(GetQuery query) {
        assertEquals(query.toString(), Graql.parse(query.toString()).toString());
    }

    private void assertEquivalent(Query query, String queryString) {
        assertEquals(queryString, query.toString());
        assertEquals(query.toString(), Graql.parse(queryString).toString());
    }
}