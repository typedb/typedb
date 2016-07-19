package io.mindmaps.graql.api.query;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.implementation.Data;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.mindmaps.graql.api.query.QueryBuilder.id;
import static io.mindmaps.graql.api.query.QueryBuilder.var;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.StringContains.containsString;

public class QueryErrorTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private static MindmapsTransaction transaction;
    private QueryBuilder qb;

    @BeforeClass
    public static void setUpClass() {
        MindmapsGraph mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
        transaction = mindmapsGraph.newTransaction();
    }

    @Before
    public void setUp() {
        qb = QueryBuilder.build(transaction);
    }

    @Test
    public void testErrorNonExistentConceptType() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("film");
        qb.match(var("x").isa("film"));
    }

    @Test
    public void testErrorNotARole() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("role"), containsString("person"), containsString("isa person")));
        qb.match(var("x").isa("movie"), var().rel("person", "y").rel("x"));
    }

    @Test
    public void testErrorNonExistentResourceType() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("thingy");
        qb.match(var("x").has("thingy", "value")).delete("x");
    }

    @Test
    public void testErrorNotARelation() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(
                containsString("relation"), containsString("movie"), containsString("separate"), containsString(";")));
        qb.match(var().isa("movie").rel("x").rel("y"));
    }

    @Test
    public void testErrorInvalidRole() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(
                containsString("relation"), containsString("has-cast"), containsString("director"),
                containsString("production-with-cast"), containsString("character-being-played"),
                containsString("actor")
        ));
        qb.match(var().isa("has-cast").rel("director", "x"));
    }

    @Test
    public void testErrorInvalidNonExistentRole() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(
                containsString("relation"), containsString("has-cast"), containsString("character-in-production"),
                containsString("production-with-cast"), containsString("character-being-played"),
                containsString("actor")
        ));
        qb.match(var().isa("has-cast").rel("character-in-production", "x"));
    }

    @Test
    public void testErrorMultipleIsa() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(
                containsString("abc"), containsString("type"), containsString("person"), containsString("has-cast"),
                containsString("separate"), containsString(";")
        ));
        qb.match(var("abc").isa("person").isa("has-cast"));
    }

    @Test
    public void testErrorHasGenreQuery() {
        // 'has genre' is not allowed because genre is an entity type
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("genre"), containsString("resource")));
        qb.match(var("x").isa("movie").has("genre", "Drama"));
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
    public void testExceptionWhenSelectVariableNotInQuery() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("$x"), containsString("match")));
        qb.match(var("y").isa("movie")).select("x");
    }

    @Test
    public void testExceptionWhenNullValue() {
        exception.expect(NullPointerException.class);
        var("x").value(null);
    }

    @Test
    public void testExceptionWhenNoHasResourceRelation() {
        // Create a fresh graph, with no has-resource relationship
        MindmapsTransaction empty = MindmapsTestGraphFactory.newEmptyGraph().newTransaction();

        QueryBuilder emptyQb = QueryBuilder.build(empty);
        emptyQb.insert(
                id("person").isa("entity-type"),
                id("name").isa("resource-type").datatype(Data.STRING)
        ).execute();

        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(
                containsString("has-resource"),
                containsString("has-resource-target"),
                containsString("has-resource-value")
        ));
        emptyQb.insert(var().isa("person").has("name", "Bob")).execute();
    }

    @Test
    public void testExceptionInstanceOfRoleType() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(
                containsString("actor"),
                containsString("role")
        ));
        qb.match(var("x").isa("actor"));
    }
}
