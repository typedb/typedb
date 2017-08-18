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

package ai.grakn.graql.internal.query;

import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.matcher.GraknMatchers.variable;
import static ai.grakn.matcher.MovieMatchers.containsAllMovies;
import static ai.grakn.util.GraqlTestUtil.assertExists;
import static ai.grakn.util.GraqlTestUtil.assertNotExists;
import static org.junit.Assert.assertThat;

public class QueryBuilderTest {

    @ClassRule
    public static final SampleKBContext movieKB = SampleKBContext.preLoad(MovieKB.get());

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @After
    public void clear(){
        movieKB.rollback();
    }

    @Test
    public void testBuildQueryGraphFirst() {
        MatchQuery query = movieKB.tx().graql().match(var("x").isa("movie"));
        assertThat(query, variable("x", containsAllMovies));
    }

    @Test
    public void testBuildMatchQueryGraphLast() {
        MatchQuery query = match(var("x").isa("movie")).withTx(movieKB.tx());
        assertThat(query, variable("x", containsAllMovies));
    }

    @Test
    public void testBuildInsertQueryGraphLast() {
        assertNotExists(movieKB.tx(), var().has("title", "a-movie"));
        InsertQuery query = insert(var().has("title", "a-movie").isa("movie")).withTx(movieKB.tx());
        query.execute();
        assertExists(movieKB.tx(), var().has("title", "a-movie"));
    }

    @Test
    public void testBuildDeleteQueryGraphLast() {
        // Insert some data to delete
        movieKB.tx().graql().insert(var().has("title", "123").isa("movie")).execute();

        assertExists(movieKB.tx(), var().has("title", "123"));

        DeleteQuery query = match(var("x").has("title", "123")).delete("x").withTx(movieKB.tx());
        query.execute();

        assertNotExists(movieKB.tx(), var().has("title", "123"));
    }

    @Test
    public void testBuildMatchInsertQueryGraphLast() {
        assertNotExists(movieKB.tx(), var().has("title", "a-movie"));
        InsertQuery query =
                match(var("x").label("movie")).
                insert(var().has("title", "a-movie").isa("movie")).withTx(movieKB.tx());
        query.execute();
        assertExists(movieKB.tx(), var().has("title", "a-movie"));
    }

    @Test
    public void testErrorExecuteMatchQueryWithoutGraph() {
        MatchQuery query = match(var("x").isa("movie"));
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("graph");
        //noinspection ResultOfMethodCallIgnored
        query.iterator();
    }

    @Test
    public void testErrorExecuteInsertQueryWithoutGraph() {
        InsertQuery query = insert(var().id(ConceptId.of("another-movie")).isa("movie"));
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("graph");
        query.execute();
    }

    @Test
    public void testErrorExecuteDeleteQueryWithoutGraph() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("graph");
        match(var("x").isa("movie")).delete("x").execute();
    }

    @Test
    public void testValidationWhenGraphProvided() {
        MatchQuery query = match(var("x").isa("not-a-thing"));
        exception.expect(GraqlQueryException.class);
        //noinspection ResultOfMethodCallIgnored
        query.withTx(movieKB.tx()).stream();
    }

    @Test
    public void testErrorWhenSpecifyGraphTwice() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("graph");
        //noinspection ResultOfMethodCallIgnored
        movieKB.tx().graql().match(var("x").isa("movie")).withTx(movieKB.tx()).stream();
    }
}