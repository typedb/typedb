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

import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.test.AbstractMovieGraphTest;
import ai.grakn.util.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.graql.Graql.name;
import static ai.grakn.graql.Graql.var;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class DeleteQueryTest extends AbstractMovieGraphTest {

    private QueryBuilder qb;
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        // TODO: Fix delete queries in titan
        assumeFalse(usingTitan());

        qb = graph.graql();
    }

    @Test
    public void testDeleteMultiple() {
        qb.insert(name("fake-type").isa(Schema.MetaSchema.ENTITY_TYPE.getId())).execute();
        qb.insert(var("x").isa("fake-type"), var("y").isa("fake-type")).execute();

        assertEquals(2, qb.match(var("x").isa("fake-type")).stream().count());

        qb.match(var("x").isa("fake-type")).delete("x").execute();

        assertFalse(qb.match(var().isa("fake-type")).ask().execute());
    }

    @Test
    public void testDeleteName() {
        qb.insert(
                var().isa("person")
                        .has("real-name", "Bob")
                        .has("real-name", "Robert")
                        .has("gender", "male")
        ).execute();

        assertTrue(qb.match(var().isa("person").has("real-name", "Bob")).ask().execute());
        assertTrue(qb.match(var().isa("person").has("real-name", "Robert")).ask().execute());
        assertTrue(qb.match(var().isa("person").has("gender", "male")).ask().execute());

        qb.match(var("x").has("real-name", "Bob")).delete(var("x").has("real-name")).execute();

        assertFalse(qb.match(var().isa("person").has("real-name", "Bob")).ask().execute());
        assertFalse(qb.match(var().isa("person").has("real-name", "Robert")).ask().execute());
        assertTrue(qb.match(var().isa("person").has("gender", "male")).ask().execute());

        qb.match(var("x").has("gender", "male")).delete("x").execute();
        assertFalse(qb.match(var().has("gender", "male")).ask().execute());
    }

    @Test
    public void testDeleteSpecificEdge() {
        Var actor = name("has-cast").hasRole("actor");
        Var productionWithCast = name("has-cast").hasRole("production-with-cast");

        assertTrue(qb.match(actor).ask().execute());
        assertTrue(qb.match(productionWithCast).ask().execute());

        qb.match(var("x").name("has-cast")).delete(var("x").hasRole("actor")).execute();
        assertTrue(qb.match(name("has-cast")).ask().execute());
        assertFalse(qb.match(actor).ask().execute());
        assertTrue(qb.match(productionWithCast).ask().execute());

        qb.insert(actor).execute();
        assertTrue(qb.match(actor).ask().execute());
    }

    @Test
    public void testDeleteSpecificName() {
        qb.insert(
                var().isa("person")
                        .has("real-name", "Bob")
                        .has("real-name", "Robert")
                        .has("gender", "male")
        ).execute();

        assertTrue(qb.match(var().isa("person").has("real-name", "Bob")).ask().execute());
        assertTrue(qb.match(var().isa("person").has("real-name", "Robert")).ask().execute());
        assertTrue(qb.match(var().isa("person").has("gender", "male")).ask().execute());

        qb.match(var("x").has("real-name", "Bob")).delete(var("x").has("real-name", "Robert")).execute();

        assertTrue(qb.match(var().isa("person").has("real-name", "Bob")).ask().execute());
        assertFalse(qb.match(var().isa("person").has("real-name", "Robert")).ask().execute());
        assertTrue(qb.match(var().isa("person").has("gender", "male")).ask().execute());

        qb.match(var("x").has("real-name", "Bob")).delete("x").execute();
        assertFalse(qb.match(var().has("real-name", "Bob").isa("person")).ask().execute());
    }

    @Test
    public void testErrorWhenDeleteValue() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("delet"), containsString("value")));
        qb.match(var("x").isa("movie")).delete(var("x").value()).execute();
    }
}
