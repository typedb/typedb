/*
 * Copyright (C) 2020 Grakn Labs
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

import grakn.core.graql.graph.MovieGraph;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlUndefine;
import graql.lang.statement.Statement;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static grakn.core.util.GraqlTestUtil.assertExists;
import static grakn.core.util.GraqlTestUtil.assertNotExists;
import static graql.lang.Graql.type;
import static graql.lang.Graql.var;

public class QueryBuilderIT {

    private static final Statement x = var("x");

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
        tx = session.writeTransaction();
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

    @Test (expected = GraqlSemanticException.class)
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
        exception.expect(GraqlSemanticException.class);
        //noinspection ResultOfMethodCallIgnored
        tx.stream(Graql.match(x.isa("not-a-thing")));
    }
}