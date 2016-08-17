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

package io.mindmaps.graql.query;

import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Var;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.mindmaps.constants.DataType.ConceptMeta.ENTITY_TYPE;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.*;

public class DeleteQueryTest {

    private QueryBuilder qb;
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        MindmapsGraph mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
        MindmapsTransaction transaction = mindmapsGraph.getTransaction();
        qb = QueryBuilder.build(transaction);
    }

    @Test
    public void testDeleteMultiple() {
        qb.insert(QueryBuilder.var().id("fake-type").isa(ENTITY_TYPE.getId())).execute();
        qb.insert(QueryBuilder.var().id("1").isa("fake-type"), QueryBuilder.var().id("2").isa("fake-type")).execute();

        assertEquals(2, qb.match(QueryBuilder.var("x").isa("fake-type")).stream().count());

        qb.match(QueryBuilder.var("x").isa("fake-type")).delete("x").execute();

        assertFalse(qb.match(QueryBuilder.var().isa("fake-type")).ask().execute());

        qb.match(QueryBuilder.var("x").id("http://mindmaps.io/fake-type")).delete("x");
    }

    @Test
    public void testDeleteName() {
        qb.insert(
                QueryBuilder.var().id("123").isa("person")
                        .has("real-name", "Bob")
                        .has("real-name", "Robert")
                        .has("gender", "male")
        ).execute();

        assertTrue(qb.match(QueryBuilder.var().id("123").has("real-name", "Bob")).ask().execute());
        assertTrue(qb.match(QueryBuilder.var().id("123").has("real-name", "Robert")).ask().execute());
        assertTrue(qb.match(QueryBuilder.var().id("123").has("gender", "male")).ask().execute());

        qb.match(QueryBuilder.var("x").id("123")).delete(QueryBuilder.var("x").has("real-name")).execute();

        assertFalse(qb.match(QueryBuilder.var().id("123").has("real-name", "Bob")).ask().execute());
        assertFalse(qb.match(QueryBuilder.var().id("123").has("real-name", "Robert")).ask().execute());
        assertTrue(qb.match(QueryBuilder.var().id("123").has("gender", "male")).ask().execute());

        qb.match(QueryBuilder.var("x").id("123")).delete("x").execute();
        assertFalse(qb.match(QueryBuilder.var().id("123")).ask().execute());
    }

    @Test
    public void testDeleteSpecificEdge() {
        Var actor = QueryBuilder.var().id("has-cast").hasRole("actor");
        Var productionWithCast = QueryBuilder.var().id("has-cast").hasRole("production-with-cast");

        assertTrue(qb.match(actor).ask().execute());
        assertTrue(qb.match(productionWithCast).ask().execute());

        qb.match(QueryBuilder.var("x").id("has-cast")).delete(QueryBuilder.var("x").hasRole("actor")).execute();
        assertTrue(qb.match(QueryBuilder.var().id("has-cast")).ask().execute());
        assertFalse(qb.match(actor).ask().execute());
        assertTrue(qb.match(productionWithCast).ask().execute());

        qb.insert(actor).execute();
        assertTrue(qb.match(actor).ask().execute());
    }

    @Test
    public void testDeleteSpecificName() {
        qb.insert(
                QueryBuilder.var().id("123").isa("person")
                        .has("real-name", "Bob")
                        .has("real-name", "Robert")
                        .has("gender", "male")
        ).execute();

        assertTrue(qb.match(QueryBuilder.var().id("123").has("real-name", "Bob")).ask().execute());
        assertTrue(qb.match(QueryBuilder.var().id("123").has("real-name", "Robert")).ask().execute());
        assertTrue(qb.match(QueryBuilder.var().id("123").has("gender", "male")).ask().execute());

        qb.match(QueryBuilder.var("x").id("123")).delete(QueryBuilder.var("x").has("real-name", "Robert")).execute();

        assertTrue(qb.match(QueryBuilder.var().id("123").has("real-name", "Bob")).ask().execute());
        assertFalse(qb.match(QueryBuilder.var().id("123").has("real-name", "Robert")).ask().execute());
        assertTrue(qb.match(QueryBuilder.var().id("123").has("gender", "male")).ask().execute());

        qb.match(QueryBuilder.var("x").id("123")).delete("x").execute();
        assertFalse(qb.match(QueryBuilder.var().id("123")).ask().execute());
    }

    @Test
    public void testErrorWhenDeleteValue() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("delet"), containsString("value")));
        qb.match(QueryBuilder.var("x").isa("movie")).delete(QueryBuilder.var("x").value());
    }
}
