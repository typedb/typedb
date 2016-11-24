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

package ai.grakn.test.graql.query;

import ai.grakn.graql.AskQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.test.AbstractMovieGraphTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryBuilderTest extends AbstractMovieGraphTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testBuildQueryGraphFirst() {
        MatchQuery query = graph.graql().match(var("x").isa("movie"));
        QueryUtil.assertResultsMatch(query, "x", "movie", graph.getResourceType("title"), QueryUtil.movies);
    }

    @Test
    public void testBuildMatchQueryGraphLast() {
        MatchQuery query = match(var("x").isa("movie")).withGraph(graph);
        QueryUtil.assertResultsMatch(query, "x", "movie", graph.getResourceType("title"), QueryUtil.movies);
    }

    @Test
    public void testBuildAskQueryGraphLast() {
        AskQuery query = match(var("x").isa("movie")).ask().withGraph(graph);
        assertTrue(query.execute());
    }

    @Test
    public void testBuildInsertQueryGraphLast() {
        assertFalse(graph.graql().match(var().has("title", "a-movie")).ask().execute());
        InsertQuery query = insert(var().has("title", "a-movie").isa("movie")).withGraph(graph);
        query.execute();
        assertTrue(graph.graql().match(var().has("title", "a-movie")).ask().execute());
    }

    @Test
    public void testBuildDeleteQueryGraphLast() {
        // Insert some data to delete
        graph.graql().insert(var().has("title", "123").isa("movie")).execute();

        assertTrue(graph.graql().match(var().has("title", "123")).ask().execute());

        DeleteQuery query = match(var("x").has("title", "123")).delete("x").withGraph(graph);
        query.execute();

        assertFalse(graph.graql().match(var().has("title", "123")).ask().execute());
    }

    @Test
    public void testBuildMatchInsertQueryGraphLast() {
        assertFalse(graph.graql().match(var().has("title", "a-movie")).ask().execute());
        InsertQuery query =
                match(var("x").name("movie")).
                insert(var().has("title", "a-movie").isa("movie")).withGraph(graph);
        query.execute();
        assertTrue(graph.graql().match(var().has("title", "a-movie")).ask().execute());
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
        graph.graql().match(var("x").isa("movie")).withGraph(graph).stream();
    }
}