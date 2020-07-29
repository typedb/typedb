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
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static graql.lang.Graql.var;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("Duplicates")
public class GraqlGetIT {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static GraknTestServer graknServer = new GraknTestServer();
    private static Session session;
    private Transaction tx;

    @BeforeClass
    public static void newSession() {
        session = graknServer.sessionWithNewKeyspace();
//        MovieGraph.load(session);
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
    public void test() {
        tx.execute(Graql.parse("define d sub attribute, value double;").asDefine());
        tx.execute(Graql.parse("insert $x 2.0 isa d; $y 2 isa d;").asInsert());
        tx.execute(Graql.parse("match $x isa d; get;").asGet());
        assertEquals(1,tx.execute(Graql.parse("match $x isa d; get;").asGet()).size());
    }

    @Test
    public void test2() {
        tx.execute(Graql.parse("define p sub entity, key d; d sub attribute, value double;").asDefine());
        tx.execute(Graql.parse("insert $x isa p, has d 2.0; $y isa p, has d 2;").asInsert());
        exception.expect(Exception.class);
        tx.commit();
    }

    @Test
    public void test3() {
        tx.execute(Graql.parse("define d sub attribute, value double;").asDefine());
        tx.execute(Graql.parse("insert $x 2 isa d; $y 2 isa d;").asInsert());
        assertEquals(1,tx.execute(Graql.parse("match $x isa d; get;").asGet()).size());
    }

    @Test
    public void test4() {
        tx.execute(Graql.parse("define person sub entity, has attr-double, has attr-long; attr-double sub attribute, value double; attr-long sub attribute, value long;").asDefine());
        tx.execute(Graql.parse("insert $x 2 isa attr-long; $y 2.0 isa attr-double;").asInsert());
        assertEquals(2,tx.execute(Graql.parse("match $x 2.0 isa attribute; get;").asGet()).size());
    }

    @Test
    public void testEmptyMatchThrows() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(Matchers.containsString("at least one property"));
        tx.execute(Graql.match(var()).get());
    }
}
