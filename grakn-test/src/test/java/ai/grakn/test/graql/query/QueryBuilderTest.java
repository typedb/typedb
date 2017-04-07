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

import ai.grakn.concept.ConceptId;
import ai.grakn.graphs.MovieGraph;
import ai.grakn.graql.AskQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.test.GraphContext;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.test.matcher.GraknMatchers.variable;
import static ai.grakn.test.matcher.MovieMatchers.containsAllMovies;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class QueryBuilderTest {

    @ClassRule
    public static final GraphContext movieGraph = GraphContext.preLoad(MovieGraph.get());

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @After
    public void clear(){
        movieGraph.rollback();
    }

    @Test
    public void testBuildQueryGraphFirst() {
        MatchQuery query = movieGraph.graph().graql().match(var("x").isa("movie"));
        assertThat(query, variable("x", containsAllMovies));
    }

    @Test
    public void testBuildMatchQueryGraphLast() {
        MatchQuery query = match(var("x").isa("movie")).withGraph(movieGraph.graph());
        assertThat(query, variable("x", containsAllMovies));
    }

    @Test
    public void testBuildAskQueryGraphLast() {
        AskQuery query = match(var("x").isa("movie")).ask().withGraph(movieGraph.graph());
        assertTrue(query.execute());
    }

    @Test
    public void testBuildInsertQueryGraphLast() {
        assertFalse(movieGraph.graph().graql().match(var().has("title", "a-movie")).ask().execute());
        InsertQuery query = insert(var().has("title", "a-movie").isa("movie")).withGraph(movieGraph.graph());
        query.execute();
        assertTrue(movieGraph.graph().graql().match(var().has("title", "a-movie")).ask().execute());
    }

    @Test
    public void testBuildDeleteQueryGraphLast() {
        // Insert some data to delete
        movieGraph.graph().graql().insert(var().has("title", "123").isa("movie")).execute();

        assertTrue(movieGraph.graph().graql().match(var().has("title", "123")).ask().execute());

        DeleteQuery query = match(var("x").has("title", "123")).delete("x").withGraph(movieGraph.graph());
        query.execute();

        assertFalse(movieGraph.graph().graql().match(var().has("title", "123")).ask().execute());
    }

    @Test
    public void testBuildMatchInsertQueryGraphLast() {
        assertFalse(movieGraph.graph().graql().match(var().has("title", "a-movie")).ask().execute());
        InsertQuery query =
                match(var("x").label("movie")).
                insert(var().has("title", "a-movie").isa("movie")).withGraph(movieGraph.graph());
        query.execute();
        assertTrue(movieGraph.graph().graql().match(var().has("title", "a-movie")).ask().execute());
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
        InsertQuery query = insert(var().id(ConceptId.of("another-movie")).isa("movie"));
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
        query.withGraph(movieGraph.graph()).stream();
    }

    @Test
    public void testErrorWhenSpecifyGraphTwice() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("graph");
        movieGraph.graph().graql().match(var("x").isa("movie")).withGraph(movieGraph.graph()).stream();
    }
}