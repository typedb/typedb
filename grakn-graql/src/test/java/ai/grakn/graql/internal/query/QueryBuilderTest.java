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

package ai.grakn.graql.internal.query;

import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.UndefineQuery;
import ai.grakn.graql.Var;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.undefine;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.matcher.GraknMatchers.variable;
import static ai.grakn.matcher.MovieMatchers.containsAllMovies;
import static ai.grakn.util.GraqlTestUtil.assertExists;
import static ai.grakn.util.GraqlTestUtil.assertNotExists;
import static org.junit.Assert.assertThat;

public class QueryBuilderTest {

    private static final Var x = Graql.var("x");

    @ClassRule
    public static final SampleKBContext movieKB = MovieKB.context();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @After
    public void clear(){
        movieKB.rollback();
    }

    @Test
    public void whenBuildingQueryWithGraphFirst_ItExecutes() {
        GetQuery query = movieKB.tx().graql().match(x.isa("movie")).get();
        assertThat(query, variable(x, containsAllMovies));
    }

    @Test
    public void whenBuildingMatchWithGraphLast_ItExecutes() {
        GetQuery query = match(x.isa("movie")).withTx(movieKB.tx()).get();
        assertThat(query, variable(x, containsAllMovies));
    }

    @Test
    public void whenBuildingInsertQueryWithGraphLast_ItExecutes() {
        assertNotExists(movieKB.tx(), var().has("title", "a-movie"));
        InsertQuery query = insert(var().has("title", "a-movie").isa("movie")).withTx(movieKB.tx());
        query.execute();
        assertExists(movieKB.tx(), var().has("title", "a-movie"));
    }

    @Test
    public void whenBuildingDeleteQueryWithGraphLast_ItExecutes() {
        // Insert some data to delete
        movieKB.tx().graql().insert(var().has("title", "123").isa("movie")).execute();

        assertExists(movieKB.tx(), var().has("title", "123"));

        DeleteQuery query = match(x.has("title", "123")).delete(x).withTx(movieKB.tx());
        query.execute();

        assertNotExists(movieKB.tx(), var().has("title", "123"));
    }

    @Test
    public void whenBuildingUndefineQueryWithGraphLast_ItExecutes() {
        movieKB.tx().graql().define(label("yes").sub("entity")).execute();

        UndefineQuery query = undefine(label("yes").sub("entity")).withTx(movieKB.tx());
        query.execute();
        assertNotExists(movieKB.tx(), label("yes").sub("entity"));
    }

    @Test
    public void whenBuildingMatchInsertQueryWithGraphLast_ItExecutes() {
        assertNotExists(movieKB.tx(), var().has("title", "a-movie"));
        InsertQuery query =
                match(x.label("movie")).
                insert(var().has("title", "a-movie").isa("movie")).withTx(movieKB.tx());
        query.execute();
        assertExists(movieKB.tx(), var().has("title", "a-movie"));
    }

    @Test
    public void whenExecutingAMatchWithoutAGraph_Throw() {
        Match match = match(x.isa("movie"));
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("graph");
        //noinspection ResultOfMethodCallIgnored
        match.iterator();
    }

    @Test
    public void whenExecutingAnInsertQueryWithoutAGraph_Throw() {
        InsertQuery query = insert(var().id(ConceptId.of("another-movie")).isa("movie"));
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("graph");
        query.execute();
    }

    @Test
    public void whenExecutingADeleteQueryWithoutAGraph_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("graph");
        match(x.isa("movie")).delete(x).execute();
    }

    @Test
    public void whenGraphIsProvidedAndQueryExecutedWithNonexistentType_Throw() {
        Match match = match(x.isa("not-a-thing"));
        exception.expect(GraqlQueryException.class);
        //noinspection ResultOfMethodCallIgnored
        match.withTx(movieKB.tx()).stream();
    }

    @Test
    public void whenGraphIsProvidedTwice_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("graph");
        //noinspection ResultOfMethodCallIgnored
        movieKB.tx().graql().match(x.isa("movie")).withTx(movieKB.tx()).stream();
    }
}