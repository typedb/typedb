package io.mindmaps.graql.api.query;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.mindmaps.core.implementation.DataType.ConceptMeta.ENTITY_TYPE;
import static io.mindmaps.graql.api.query.QueryBuilder.var;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.*;

public class DeleteQueryTest {

    private MindmapsTransaction transaction;
    private QueryBuilder qb;
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        MindmapsGraph mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
        transaction = mindmapsGraph.newTransaction();
        qb = QueryBuilder.build(transaction);
    }

    @Test
    public void testDeleteMultiple() {
        qb.insert(var().id("fake-type").isa(ENTITY_TYPE.getId())).execute();
        qb.insert(var().id("1").isa("fake-type"), var().id("2").isa("fake-type")).execute();

        assertEquals(2, qb.match(var("x").isa("fake-type")).stream().count());

        qb.match(var("x").isa("fake-type")).delete("x").execute();

        assertFalse(qb.match(var().isa("fake-type")).ask().execute());

        qb.match(var("x").id("http://mindmaps.io/fake-type")).delete("x");
    }

    @Test
    public void testDeleteName() {
        qb.insert(
                var().id("123").isa("person")
                        .has("real-name", "Bob")
                        .has("real-name", "Robert")
                        .has("gender", "male")
        ).execute();

        assertTrue(qb.match(var().id("123").has("real-name", "Bob")).ask().execute());
        assertTrue(qb.match(var().id("123").has("real-name", "Robert")).ask().execute());
        assertTrue(qb.match(var().id("123").has("gender", "male")).ask().execute());

        qb.match(var("x").id("123")).delete(var("x").has("real-name")).execute();

        assertFalse(qb.match(var().id("123").has("real-name", "Bob")).ask().execute());
        assertFalse(qb.match(var().id("123").has("real-name", "Robert")).ask().execute());
        assertTrue(qb.match(var().id("123").has("gender", "male")).ask().execute());

        qb.match(var("x").id("123")).delete("x").execute();
        assertFalse(qb.match(var().id("123")).ask().execute());
    }

    @Test
    public void testDeleteSpecificEdge() {
        Var actor = var().id("has-cast").hasRole("actor");
        Var productionWithCast = var().id("has-cast").hasRole("production-with-cast");

        assertTrue(qb.match(actor).ask().execute());
        assertTrue(qb.match(productionWithCast).ask().execute());

        qb.match(var("x").id("has-cast")).delete(var("x").hasRole("actor")).execute();
        assertTrue(qb.match(var().id("has-cast")).ask().execute());
        assertFalse(qb.match(actor).ask().execute());
        assertTrue(qb.match(productionWithCast).ask().execute());

        qb.insert(actor).execute();
        assertTrue(qb.match(actor).ask().execute());
    }

    @Test
    public void testDeleteSpecificName() {
        qb.insert(
                var().id("123").isa("person")
                        .has("real-name", "Bob")
                        .has("real-name", "Robert")
                        .has("gender", "male")
        ).execute();

        assertTrue(qb.match(var().id("123").has("real-name", "Bob")).ask().execute());
        assertTrue(qb.match(var().id("123").has("real-name", "Robert")).ask().execute());
        assertTrue(qb.match(var().id("123").has("gender", "male")).ask().execute());

        qb.match(var("x").id("123")).delete(var("x").has("real-name", "Robert")).execute();

        assertTrue(qb.match(var().id("123").has("real-name", "Bob")).ask().execute());
        assertFalse(qb.match(var().id("123").has("real-name", "Robert")).ask().execute());
        assertTrue(qb.match(var().id("123").has("gender", "male")).ask().execute());

        qb.match(var("x").id("123")).delete("x").execute();
        assertFalse(qb.match(var().id("123")).ask().execute());
    }

    @Test
    public void testErrorWhenDeleteValue() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("delet"), containsString("value")));
        qb.match(var("x").isa("movie")).delete(var("x").value());
    }
}
