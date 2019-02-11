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

import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.graql.query.query.GraqlDelete;
import grakn.core.graql.query.query.GraqlInsert;
import grakn.core.graql.query.query.GraqlUndefine;
import grakn.core.graql.query.statement.Statement;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static grakn.core.graql.query.Graql.type;
import static grakn.core.graql.query.Graql.var;
import static grakn.core.util.GraqlTestUtil.assertExists;
import static grakn.core.util.GraqlTestUtil.assertNotExists;

public class QueryBuilderIT {

    private static final Statement x = var("x");

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();
    private static SessionImpl session;
    private TransactionOLTP tx;

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
        GraqlInsert query = Graql.insert(var().has("title", "a-movie").isa("movie"));
        tx.execute(query);
        assertExists(tx, var().has("title", "a-movie"));
    }

    @Test
    public void whenBuildingDeleteQueryWithGraphLast_ItExecutes() {
        // Insert some data to delete
        tx.execute(Graql.insert(var().has("title", "123").isa("movie")));

        assertExists(tx, var().has("title", "123"));

        GraqlDelete query = Graql.match(x.has("title", "123")).delete(x.var());
        tx.execute(query);

        assertNotExists(tx, var().has("title", "123"));
    }

    @Test
    public void whenBuildingUndefineQueryWithGraphLast_ItExecutes() {
        tx.execute(Graql.define(type("yes").sub("entity")));

        GraqlUndefine query = Graql.undefine(type("yes").sub("entity"));
        tx.execute(query);
        assertNotExists(tx, type("yes").sub("entity"));
    }

    @Test
    public void whenBuildingMatchInsertQueryWithGraphLast_ItExecutes() {
        assertNotExists(tx, var().has("title", "a-movie"));
        GraqlInsert query =
                Graql.match(x.type("movie"))
                        .insert(var().has("title", "a-movie").isa("movie"));
        tx.execute(query);
        assertExists(tx, var().has("title", "a-movie"));
    }

    @Test
    public void whenGraphIsProvidedAndQueryExecutedWithNonexistentType_Throw() {
        exception.expect(GraqlQueryException.class);
        //noinspection ResultOfMethodCallIgnored
        tx.stream(Graql.match(x.isa("not-a-thing")));
    }
}