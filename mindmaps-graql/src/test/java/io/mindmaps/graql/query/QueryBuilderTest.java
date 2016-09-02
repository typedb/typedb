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
import io.mindmaps.MindmapsTransaction;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.graql.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.mindmaps.graql.Graql.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore
public class QueryBuilderTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private static MindmapsTransaction transaction;

    @Before
    public void setUp() {
        MindmapsGraph mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
        transaction = mindmapsGraph.getTransaction();
    }

    @Test
    public void testBuildQueryTransactionFirst() {
        MatchQueryDefault query = withTransaction(transaction).match(var("x").isa("movie"));
        QueryUtil.assertResultsMatch(query, "x", "movie", QueryUtil.movies);
    }

    @Test
    public void testBuildMatchQueryTransactionLast() {
        MatchQueryDefault query = match(var("x").isa("movie")).withTransaction(transaction);
        QueryUtil.assertResultsMatch(query, "x", "movie", QueryUtil.movies);
    }

    @Test
    public void testBuildAskQueryTransactionLast() {
        AskQuery query = match(var("x").isa("movie")).ask().withTransaction(transaction);
        assertTrue(query.execute());
    }

    @Test
    public void testBuildInsertQueryTransactionLast() {
        assertFalse(withTransaction(transaction).match(var().id("a-movie")).ask().execute());
        InsertQuery query = insert(var().id("a-movie").isa("movie")).withTransaction(transaction);
        query.execute();
        assertTrue(withTransaction(transaction).match(var().id("a-movie")).ask().execute());
    }

    @Test
    public void testBuildDeleteQueryTransactionLast() {
        // Insert some data to delete
        withTransaction(transaction).insert(var().id("123").isa("movie")).execute();

        assertTrue(withTransaction(transaction).match(var().id("123")).ask().execute());

        DeleteQuery query = match(var("x").id("123")).delete("x").withTransaction(transaction);
        query.execute();

        assertFalse(withTransaction(transaction).match(var().id("123")).ask().execute());
    }

    @Test
    public void testBuildMatchInsertQueryTransactionLast() {
        assertFalse(withTransaction(transaction).match(var().id("a-movie")).ask().execute());
        InsertQuery query =
                match(var("x").id("movie")).
                insert(var().id("a-movie").isa("movie")).withTransaction(transaction);
        query.execute();
        assertTrue(withTransaction(transaction).match(var().id("a-movie")).ask().execute());
    }

    @Test
    public void testErrorExecuteMatchQueryWithoutTransaction() {
        MatchQueryDefault query = match(var("x").isa("movie"));
        exception.expect(IllegalStateException.class);
        exception.expectMessage("transaction");
        query.iterator();
    }

    @Test
    public void testErrorExecuteAskQueryWithoutTransaction() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("transaction");
        match(var("x").isa("movie")).ask().execute();
    }

    @Test
    public void testErrorExecuteInsertQueryWithoutTransaction() {
        InsertQuery query = insert(var().id("another-movie").isa("movie"));
        exception.expect(IllegalStateException.class);
        exception.expectMessage("transaction");
        query.execute();
    }

    @Test
    public void testErrorExecuteDeleteQueryWithoutTransaction() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("transaction");
        match(var("x").isa("movie")).delete("x").execute();
    }

    @Test
    public void testValidationWhenTransactionProvided() {
        MatchQueryDefault query = match(var("x").isa("not-a-thing"));
        exception.expect(IllegalStateException.class);
        query.withTransaction(transaction).stream();
    }

    @Test
    public void testErrorWhenSpecifyTransactionTwice() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("transaction");
        withTransaction(transaction).match(var("x").isa("movie")).withTransaction(transaction).stream();
    }
}