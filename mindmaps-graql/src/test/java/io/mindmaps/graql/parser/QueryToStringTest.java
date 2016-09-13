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

package io.mindmaps.graql.parser;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.graql.ComputeQuery;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import org.junit.Before;
import org.junit.Test;

import static io.mindmaps.graql.Graql.*;
import static org.junit.Assert.assertEquals;

public class QueryToStringTest {

    private QueryBuilder qb;

    @Before
    public void setUp() {
        MindmapsGraph mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
        qb = withGraph(mindmapsGraph);
    }

    @Test
    public void testSimpleMatchQueryToString() {
        assertValidToString(qb.match(var("x").isa("movie").id("Godfather")));
    }

    @Test
    public void testComplexQueryToString() {
        MatchQuery query = qb.match(
                var("x").isa("movie"),
                var().rel("x").rel("y"),
                or(
                        var("y").isa("person"),
                        var("y").isa("genre").value(neq("crime"))
                )
        ).select("x", "y").orderBy("y").limit(8).offset(4);
        assertValidToString(query);
    }

    @Test
    public void testQueryWithResourcesToString() {
        assertValidToString(qb.match(var("x").has("tmdb-vote-count", lte(400))));
    }

    @Test
    public void testQueryWithAkoToString() {
        assertValidToString(qb.match(var("x").ako(var("y"))));
    }

    @Test
    public void testQueryWithPlaysRoleToString() {
        assertValidToString(qb.match(var("x").playsRole(var("y"))));
    }

    @Test
    public void testQueryWithHasRoleToString() {
        assertValidToString(qb.match(var("x").hasRole(var("y"))));
    }

    @Test
    public void testQueryWithHasScopeToString() {
        assertEquals("match $x has-scope $y", qb.match(var("x").hasScope(var("y"))).toString());
    }

    @Test
    public void testQueryWithDatatypeToString() {
        assertValidToString(qb.match(var("x").datatype(ResourceType.DataType.LONG)));
    }

    @Test
    public void testQueryIsAbstractToString() {
        assertValidToString(qb.match(var("x").isAbstract()));
    }

    @Test
    public void testQueryWithRhsToString() {
        assertValidToString(qb.match(var("x").rhs("match $x isa movie delete $x")));
    }

    @Test
    public void testQueryWithLhsToString() {
        assertValidToString(qb.match(var("x").lhs("match $x isa person ask")));
    }

    @Test
    public void testInsertQueryToString() {
        assertEquals("insert $x isa movie;", qb.insert(var("x").isa("movie")).toString());
    }

    @Test
    public void testEscapeStrings() {
        assertEquals("insert $x value \"hello\\nworld\";", qb.insert(var("x").value("hello\nworld")).toString());
    }

    @Test
    public void testQuoteIds() {
        assertEquals(
                "match $a (\"hello\\tworld\")",
                match(var("a").rel(id("hello\tworld"))).toString()
        );
    }

    @Test
    public void testQuoteIdsNumbers() {
        assertEquals(
                "match $a (\"1hi\")",
                match(var("a").rel(id("1hi"))).toString()
        );
    }

    @Test
    public void testHasResource() {
        assertEquals("insert $x has-resource thingy;", qb.insert(var("x").hasResource("thingy")).toString());
    }

    @Test
    public void testComputeQueryToString() {
        assertEquals("compute count", qb.compute("count").toString());
    }

    @Test
    public void testComputeQuerySubgraphToString() {
        ComputeQuery query = qb.compute("degrees", Sets.newHashSet("movie", "person"));
        assertEquals("compute degrees in movie, person", query.toString());
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testToStringUnsupported() {
        //noinspection ResultOfMethodCallIgnored
        qb.match(var("x").isa(var().value("abc"))).toString();
    }

    private void assertValidToString(MatchQuery query) {
        QueryParserTest.assertQueriesEqual(query, qb.parseMatch(query.toString()));
    }
}
