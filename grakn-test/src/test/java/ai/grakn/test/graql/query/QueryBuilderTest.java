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

package ai.grakn.test.graql.query;

import ai.grakn.graql.AskQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.test.AbstractMovieGraphTest;
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
import static ai.grakn.graql.Graql.withGraph;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryBuilderTest extends AbstractMovieGraphTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testBuildQueryGraphFirst() {
        MatchQuery query = Graql.withGraph(graph).match(Graql.var("x").isa("movie"));
        QueryUtil.assertResultsMatch(query, "x", "movie", QueryUtil.movies);
    }

    @Test
    public void testBuildMatchQueryGraphLast() {
        MatchQuery query = Graql.match(Graql.var("x").isa("movie")).withGraph(graph);
        QueryUtil.assertResultsMatch(query, "x", "movie", QueryUtil.movies);
    }

    @Test
    public void testBuildAskQueryGraphLast() {
        AskQuery query = Graql.match(Graql.var("x").isa("movie")).ask().withGraph(graph);
        assertTrue(query.execute());
    }

    @Test
    public void testBuildInsertQueryGraphLast() {
        assertFalse(Graql.withGraph(graph).match(Graql.var().id("a-movie")).ask().execute());
        InsertQuery query = Graql.insert(Graql.var().id("a-movie").isa("movie")).withGraph(graph);
        query.execute();
        assertTrue(Graql.withGraph(graph).match(Graql.var().id("a-movie")).ask().execute());
    }

    @Test
    public void testBuildDeleteQueryGraphLast() {
        // Insert some data to delete
        Graql.withGraph(graph).insert(Graql.var().id("123").isa("movie")).execute();

        assertTrue(Graql.withGraph(graph).match(Graql.var().id("123")).ask().execute());

        DeleteQuery query = Graql.match(Graql.var("x").id("123")).delete("x").withGraph(graph);
        query.execute();

        assertFalse(Graql.withGraph(graph).match(Graql.var().id("123")).ask().execute());
    }

    @Test
    public void testBuildMatchInsertQueryGraphLast() {
        assertFalse(Graql.withGraph(graph).match(Graql.var().id("a-movie")).ask().execute());
        InsertQuery query =
                Graql.match(Graql.var("x").id("movie")).
                insert(Graql.var().id("a-movie").isa("movie")).withGraph(graph);
        query.execute();
        assertTrue(Graql.withGraph(graph).match(Graql.var().id("a-movie")).ask().execute());
    }

    @Test
    public void testErrorExecuteMatchQueryWithoutGraph() {
        MatchQuery query = Graql.match(Graql.var("x").isa("movie"));
        exception.expect(IllegalStateException.class);
        exception.expectMessage("graph");
        query.iterator();
    }

    @Test
    public void testErrorExecuteAskQueryWithoutGraph() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("graph");
        Graql.match(Graql.var("x").isa("movie")).ask().execute();
    }

    @Test
    public void testErrorExecuteInsertQueryWithoutGraph() {
        InsertQuery query = Graql.insert(Graql.var().id("another-movie").isa("movie"));
        exception.expect(IllegalStateException.class);
        exception.expectMessage("graph");
        query.execute();
    }

    @Test
    public void testErrorExecuteDeleteQueryWithoutGraph() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("graph");
        Graql.match(Graql.var("x").isa("movie")).delete("x").execute();
    }

    @Test
    public void testValidationWhenGraphProvided() {
        MatchQuery query = Graql.match(Graql.var("x").isa("not-a-thing"));
        exception.expect(IllegalStateException.class);
        query.withGraph(graph).stream();
    }

    @Test
    public void testErrorWhenSpecifyGraphTwice() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("graph");
        Graql.withGraph(graph).match(Graql.var("x").isa("movie")).withGraph(graph).stream();
    }
}