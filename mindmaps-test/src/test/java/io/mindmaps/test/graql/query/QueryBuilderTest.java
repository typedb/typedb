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

package io.mindmaps.test.graql.query;

import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.graql.AskQuery;
import io.mindmaps.graql.DeleteQuery;
import io.mindmaps.graql.InsertQuery;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.test.AbstractMindmapsEngineTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.mindmaps.graql.Graql.insert;
import static io.mindmaps.graql.Graql.match;
import static io.mindmaps.graql.Graql.var;
import static io.mindmaps.graql.Graql.withGraph;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryBuilderTest extends AbstractMindmapsEngineTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        MovieGraphFactory.loadGraph(graph);
    }

    @Test
    public void testBuildQueryGraphFirst() {
        MatchQuery query = withGraph(graph).match(var("x").isa("movie"));
        QueryUtil.assertResultsMatch(query, "x", "movie", QueryUtil.movies);
    }

    @Test
    public void testBuildMatchQueryGraphLast() {
        MatchQuery query = match(var("x").isa("movie")).withGraph(graph);
        QueryUtil.assertResultsMatch(query, "x", "movie", QueryUtil.movies);
    }

    @Test
    public void testBuildAskQueryGraphLast() {
        AskQuery query = match(var("x").isa("movie")).ask().withGraph(graph);
        assertTrue(query.execute());
    }

    @Test
    public void testBuildInsertQueryGraphLast() {
        assertFalse(withGraph(graph).match(var().id("a-movie")).ask().execute());
        InsertQuery query = insert(var().id("a-movie").isa("movie")).withGraph(graph);
        query.execute();
        assertTrue(withGraph(graph).match(var().id("a-movie")).ask().execute());
    }

    @Test
    public void testBuildDeleteQueryGraphLast() {
        // Insert some data to delete
        withGraph(graph).insert(var().id("123").isa("movie")).execute();

        assertTrue(withGraph(graph).match(var().id("123")).ask().execute());

        DeleteQuery query = match(var("x").id("123")).delete("x").withGraph(graph);
        query.execute();

        assertFalse(withGraph(graph).match(var().id("123")).ask().execute());
    }

    @Test
    public void testBuildMatchInsertQueryGraphLast() {
        assertFalse(withGraph(graph).match(var().id("a-movie")).ask().execute());
        InsertQuery query =
                match(var("x").id("movie")).
                insert(var().id("a-movie").isa("movie")).withGraph(graph);
        query.execute();
        assertTrue(withGraph(graph).match(var().id("a-movie")).ask().execute());
    }

    @Test
    public void testErrorExecuteMatchQueryWithoutGraph() {
        MatchQuery query = match(var("x").isa("movie"));
        exception.expect(IllegalStateException.class);
        exception.expectMessage("graph");
        query.iterator();
    }

    @Test
    public void testErrorExecuteAskQueryWithoutGraph() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("graph");
        match(var("x").isa("movie")).ask().execute();
    }

    @Test
    public void testErrorExecuteInsertQueryWithoutGraph() {
        InsertQuery query = insert(var().id("another-movie").isa("movie"));
        exception.expect(IllegalStateException.class);
        exception.expectMessage("graph");
        query.execute();
    }

    @Test
    public void testErrorExecuteDeleteQueryWithoutGraph() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("graph");
        match(var("x").isa("movie")).delete("x").execute();
    }

    @Test
    public void testValidationWhenGraphProvided() {
        MatchQuery query = match(var("x").isa("not-a-thing"));
        exception.expect(IllegalStateException.class);
        query.withGraph(graph).stream();
    }

    @Test
    public void testErrorWhenSpecifyGraphTwice() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("graph");
        withGraph(graph).match(var("x").isa("movie")).withGraph(graph).stream();
    }
}