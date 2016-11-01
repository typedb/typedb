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

import ai.grakn.GraknGraph;
import ai.grakn.concept.ResourceType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.AbstractMovieGraphTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.grakn.graql.Graql.id;
import static io.grakn.graql.Graql.var;
import static io.grakn.graql.Graql.withGraph;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.StringContains.containsString;

public class QueryErrorTest extends AbstractMovieGraphTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private QueryBuilder qb;

    @Before
    public void setUp() {
        qb = withGraph(graph);
    }

    @Test
    public void testErrorNonExistentConceptType() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("film");
        qb.match(var("x").isa("film")).stream();
    }

    @Test
    public void testErrorNotARole() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("role"), containsString("person"), containsString("isa person")));
        qb.match(var("x").isa("movie"), var().rel("person", "y").rel("x")).stream();
    }

    @Test
    public void testErrorNonExistentResourceType() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("thingy");
        qb.match(var("x").has("thingy", "value")).delete("x").execute();
    }

    @Test
    public void testErrorNotARelation() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(
                containsString("relation"), containsString("movie"), containsString("separate"), containsString(";")));
        qb.match(var().isa("movie").rel("x").rel("y")).stream();
    }

    @Test
    public void testErrorInvalidRole() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(
                containsString("relation"), containsString("has-cast"), containsString("director"),
                containsString("production-with-cast"), containsString("character-being-played"),
                containsString("actor")
        ));
        qb.match(var().isa("has-cast").rel("director", "x")).stream();
    }

    @Test
    public void testErrorInvalidNonExistentRole() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(
                containsString("relation"), containsString("has-cast"), containsString("character-in-production"),
                containsString("production-with-cast"), containsString("character-being-played"),
                containsString("actor")
        ));
        qb.match(var().isa("has-cast").rel("character-in-production", "x")).stream();
    }

    @Test
    public void testErrorMultipleIsa() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(
                containsString("abc"), containsString("isa"), containsString("person"), containsString("has-cast")
        ));
        qb.match(var("abc").isa("person").isa("has-cast"));
    }

    @Test
    public void testErrorHasGenreQuery() {
        // 'has genre' is not allowed because genre is an entity type
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("genre"), containsString("resource")));
        qb.match(var("x").isa("movie").has("genre", "Drama")).stream();
    }

    @Test
    public void testExceptionWhenNoSelectVariablesProvided() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(containsString("select"));
        qb.match(var("x").isa("movie")).select();
    }

    @Test
    public void testExceptionWhenNoPatternsProvided() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(allOf(containsString("match"), containsString("pattern")));
        qb.match();
    }

    @Test
    public void testExceptionWhenNullValue() {
        exception.expect(NullPointerException.class);
        var("x").value(null);
    }

    @Test
    public void testExceptionWhenNoHasResourceRelation() throws GraknValidationException {
        // Create a fresh graph, with no has-resource between person and name
        GraknGraph empty = factoryWithNewKeyspace().getGraph();

        QueryBuilder emptyQb = withGraph(empty);
        emptyQb.insert(
                id("person").isa("entity-type"),
                id("name").isa("resource-type").datatype(ResourceType.DataType.STRING)
        ).execute();

        exception.expect(GraknValidationException.class);
        exception.expectMessage(allOf(
                containsString("person"),
                containsString("name")
        ));
        emptyQb.insert(var().isa("person").has("name", "Bob")).execute();

        empty.commit();
    }

    @Test
    public void testExceptionInstanceOfRoleType() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(
                containsString("actor"),
                containsString("role")
        ));
        qb.match(var("x").isa("actor")).stream();
    }

    @Test
    public void testExceptionOrderTwice() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("order");
        qb.match(var("x").isa("movie")).orderBy("x").orderBy("x").stream();
    }
}
