package io.mindmaps.graql.api.query;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.mindmaps.graql.api.query.QueryBuilder.var;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryBuilderTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private static MindmapsTransaction transaction;

    @Before
    public void setUp() {
        MindmapsGraph mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
        transaction = mindmapsGraph.newTransaction();
    }

    @Test
    public void testBuildQueryTransactionFirst() {
        MatchQuery query = QueryBuilder.build(transaction).match(var("x").isa("movie"));
        QueryUtil.assertResultsMatch(query, "x", "movie", QueryUtil.movies);
    }

    @Test
    public void testBuildMatchQueryTransactionLast() {
        MatchQuery query = QueryBuilder.build().match(var("x").isa("movie")).withTransaction(transaction);
        QueryUtil.assertResultsMatch(query, "x", "movie", QueryUtil.movies);
    }

    @Test
    public void testBuildAskQueryTransactionLast() {
        AskQuery query = QueryBuilder.build().match(var("x").isa("movie")).ask().withTransaction(transaction);
        assertTrue(query.execute());
    }

    @Test
    public void testBuildInsertQueryTransactionLast() {
        assertFalse(QueryBuilder.build(transaction).match(var().id("a-movie")).ask().execute());
        InsertQuery query = QueryBuilder.build().insert(var().id("a-movie").isa("movie")).withTransaction(transaction);
        query.execute();
        assertTrue(QueryBuilder.build(transaction).match(var().id("a-movie")).ask().execute());
    }

    @Test
    public void testBuildDeleteQueryTransactionLast() {
        // Insert some data to delete
        QueryBuilder.build(transaction).insert(var().id("123").isa("movie")).execute();

        assertTrue(QueryBuilder.build(transaction).match(var().id("123")).ask().execute());

        DeleteQuery query = QueryBuilder.build().match(var("x").id("123")).delete("x").withTransaction(transaction);
        query.execute();

        assertFalse(QueryBuilder.build(transaction).match(var().id("123")).ask().execute());
    }

    @Test
    public void testErrorExecuteMatchQueryWithoutTransaction() {
        MatchQuery query = QueryBuilder.build().match(var("x").isa("movie"));
        exception.expect(IllegalStateException.class);
        exception.expectMessage("transaction");
        query.iterator();
    }

    @Test
    public void testErrorExecuteAskQueryWithoutTransaction() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("transaction");
        QueryBuilder.build().match(var("x").isa("movie")).ask().execute();
    }

    @Test
    public void testErrorExecuteInsertQueryWithoutTransaction() {
        InsertQuery query = QueryBuilder.build().insert(var().id("another-movie").isa("movie"));
        exception.expect(IllegalStateException.class);
        exception.expectMessage("transaction");
        query.execute();
    }

    @Test
    public void testErrorExecuteDeleteQueryWithoutTransaction() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("transaction");
        QueryBuilder.build().match(var("x").isa("movie")).delete("x").execute();
    }

    @Test
    public void testValidationWhenTransactionProvided() {
        MatchQuery query = QueryBuilder.build().match(var("x").isa("not-a-thing"));
        exception.expect(IllegalStateException.class);
        query.withTransaction(transaction);
    }
}