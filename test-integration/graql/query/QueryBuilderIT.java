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

package grakn.core.graql.query;

import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static grakn.core.graql.query.Graql.insert;
import static grakn.core.graql.query.Graql.match;
import static grakn.core.graql.query.pattern.Pattern.label;
import static grakn.core.graql.query.pattern.Pattern.var;
import static grakn.core.util.GraqlTestUtil.assertExists;
import static grakn.core.util.GraqlTestUtil.assertNotExists;

public class QueryBuilderIT {

    private static final Variable x = var("x");

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();
    private static Session session;
    private Transaction tx;

    @BeforeClass
    public static void newSession() {
        session = graknServer.sessionWithNewKeyspace();
        MovieGraph.load(session);
    }

    @Before
    public void newTransaction() {
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @After
    public void closeTransaction() {
        tx.close();
    }

    @AfterClass
    public static void closeSession() {
        session.close();
    }

    @Test
    public void whenBuildingInsertQueryWithGraphLast_ItExecutes() {
        assertNotExists(tx, var().has("title", "a-movie"));
        InsertQuery query = tx.graql().insert(var().has("title", "a-movie").isa("movie"));
        query.execute();
        assertExists(tx, var().has("title", "a-movie"));
    }

    @Test
    public void whenBuildingDeleteQueryWithGraphLast_ItExecutes() {
        // Insert some data to delete
        tx.graql().insert(var().has("title", "123").isa("movie")).execute();

        assertExists(tx, var().has("title", "123"));

        DeleteQuery query = tx.graql().match(x.has("title", "123")).delete(x);
        query.execute();

        assertNotExists(tx, var().has("title", "123"));
    }

    @Test
    public void whenBuildingUndefineQueryWithGraphLast_ItExecutes() {
        tx.graql().define(label("yes").sub("entity")).execute();

        UndefineQuery query = tx.graql().undefine(label("yes").sub("entity"));
        query.execute();
        assertNotExists(tx, label("yes").sub("entity"));
    }

    @Test
    public void whenBuildingMatchInsertQueryWithGraphLast_ItExecutes() {
        assertNotExists(tx, var().has("title", "a-movie"));
        InsertQuery query =
                tx.graql().match(x.label("movie"))
                        .insert(var().has("title", "a-movie").isa("movie"));
        query.execute();
        assertExists(tx, var().has("title", "a-movie"));
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
        exception.expect(GraqlQueryException.class);
        //noinspection ResultOfMethodCallIgnored
        tx.graql().match(x.isa("not-a-thing")).stream();
    }
}