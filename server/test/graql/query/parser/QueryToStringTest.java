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

package grakn.core.graql.query.parser;

import grakn.core.graql.query.query.GraqlCompute;
import grakn.core.graql.query.query.GraqlGet;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.query.GraqlInsert;
import grakn.core.graql.query.query.MatchClause;
import grakn.core.graql.query.query.GraqlQuery;
import graql.lang.util.Token;
import org.junit.Test;

import static graql.lang.util.Token.Compute.Algorithm.CONNECTED_COMPONENT;
import static graql.lang.util.Token.Compute.Algorithm.DEGREE;
import static graql.lang.util.Token.Compute.Algorithm.K_CORE;
import static grakn.core.graql.query.query.GraqlCompute.Argument.k;
import static grakn.core.graql.query.query.GraqlCompute.Argument.min_k;
import static grakn.core.graql.query.query.GraqlCompute.Argument.size;
import static grakn.core.graql.query.Graql.and;
import static grakn.core.graql.query.Graql.lte;
import static grakn.core.graql.query.Graql.match;
import static grakn.core.graql.query.Graql.or;
import static grakn.core.graql.query.Graql.rel;
import static grakn.core.graql.query.Graql.type;
import static grakn.core.graql.query.Graql.var;
import static org.junit.Assert.assertEquals;

public class QueryToStringTest {

    @Test
    public void testSimpleGetQueryToString() {
        assertSameStringRepresentation(Graql.match(var("x").isa("movie").has("title", "Godfather")).get());
    }

    @Test
    public void testComplexQueryToString() {
        GraqlGet query = Graql.match(
                var("x").isa("movie"),
                var().rel("x").rel("y"),
                or(
                        var("y").isa("person"),
                        var("y").isa("genre").neq("crime")
                ),
                var("y").has("name", var("n"))
        ).get("x", "y", "n").sort("n").offset(4).limit(8);
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
        assertSameStringRepresentation(Graql.match(var("x").datatype(Token.DataType.LONG)).get());
    }

    @Test
    public void testQueryIsAbstractToString() {
        assertSameStringRepresentation(Graql.match(var("x").isAbstract()).get());
    }

    @Test
    public void testQueryWithThenToString() {
        assertValidToString(Graql.define(type("a-rule").sub("rule").then(and(Graql.parsePatternList("$x isa movie;")))));
    }

    @Test
    public void testQueryWithWhenToString() {
        assertValidToString(Graql.define(type("a-rule").sub("rule").when(and(Graql.parsePatternList("$x isa movie;")))));
    }

    private void assertValidToString(GraqlQuery query) {
        //No need to execute the insert query
        GraqlQuery parsedQuery = Graql.parse(query.toString());
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
        assertEquals("compute count;", Graql.compute().count().toString());
    }

    @Test
    public void testComputeQuerySubgraphToString() {
        GraqlCompute query = Graql.compute().centrality().using(DEGREE).in("movie", "person");
        assertEquivalent(query, "compute centrality in [movie, person], using degree;");
    }

    @Test
    public void testClusterToString() {
        GraqlCompute connectedcomponent = Graql.compute().cluster().using(CONNECTED_COMPONENT).in("movie", "person");
        assertEquivalent(connectedcomponent, "compute cluster in [movie, person], using connected-component;");

        GraqlCompute kcore = Graql.compute().cluster().using(K_CORE).in("movie", "person");
        assertEquivalent(kcore, "compute cluster in [movie, person], using k-core;");
    }

    @Test
    public void testCCSizeToString() {
        GraqlCompute query = Graql.compute().cluster().using(CONNECTED_COMPONENT).in("movie", "person").where(size(10));
        assertEquivalent(query, "compute cluster in [movie, person], using connected-component, where size=10;");
    }

    @Test
    public void testKCoreToString() {
        GraqlCompute query = Graql.compute().cluster().using(K_CORE).in("movie", "person").where(k(10));
        assertEquivalent(query, "compute cluster in [movie, person], using k-core, where k=10;");
    }

    @Test
    public void testCentralityOf() {
        GraqlCompute query = Graql.compute().centrality().using(DEGREE).in("movie", "person").of("person");
        assertEquivalent(query, "compute centrality of person, in [movie, person], using degree;");

        query = Graql.compute().centrality().using(K_CORE).in("movie", "person").of("person").where(min_k(5));
        assertEquivalent(query, "compute centrality of person, in [movie, person], using k-core, where min-k=5;");
    }

    @Test
    public void testQueryToStringWithReservedKeywords() {
        GraqlGet query = Graql.match(var("x").isa("isa")).get("x");
        assertEquals("match $x isa \"isa\"; get $x;", query.toString());
    }

    @Test
    public void testRepeatRoleplayerToString() {
        assertEquals("match ($x, $x);", match(rel("x").rel("x")).toString());
    }

    @Test
    public void testMatchInsertToString() {
        GraqlInsert query = Graql.match(var("x").isa("movie")).insert(var("x").has("title", "hello"));
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
        MatchClause match = match(var("x").contains(var("y")));

        assertEquals("match $x contains $y;", match.toString());
    }

    private void assertSameStringRepresentation(GraqlGet query) {
        assertEquals(query.toString(), Graql.parse(query.toString()).toString());
    }

    private void assertEquivalent(GraqlQuery query, String queryString) {
        assertEquals(queryString, query.toString());
        assertEquals(query.toString(), Graql.parse(queryString).toString());
    }
}