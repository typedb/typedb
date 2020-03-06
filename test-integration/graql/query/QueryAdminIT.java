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

import com.google.common.collect.Sets;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlInsert;
import graql.lang.query.MatchClause;
import graql.lang.statement.Variable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import static graql.lang.Graql.var;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class QueryAdminIT {

    @ClassRule
    public static GraknTestServer graknServer = new GraknTestServer();
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
    public void testDefaultGetSelectedNamesInQuery() {
        MatchClause match = Graql.match(var("x").isa(var("y")));

        assertEquals(Sets.newHashSet(new Variable("x"), new Variable("y")), match.getSelectedNames());
    }

    @Test
    public void testGetPatternInQuery() {
        MatchClause match = Graql.match(var("x").isa("movie"), var("x").val("Bob"));

        Conjunction<Pattern> conjunction = match.getPatterns();
        assertNotNull(conjunction);

        Set<Pattern> patterns = conjunction.getPatterns();
        assertEquals(2, patterns.size());
    }

    @Test
    public void testMutateMatch() {
        MatchClause match = Graql.match(var("x").isa("movie"));

        Conjunction<Pattern> pattern = match.getPatterns();
        pattern.getPatterns().add(var("x").has("title", "Spy"));

        assertEquals(1, tx.stream(match).count());
    }

    @Test
    public void testInsertQueryMatchPatternEmpty() {
        GraqlInsert query = Graql.insert(var().id("123").isa("movie"));
        assertNull(query.match());
    }

    @Test
    public void testInsertQueryWithMatch() {
        GraqlInsert query = Graql.match(var("x").isa("movie")).insert(var().id("123").isa("movie"));
        assertEquals("match $x isa movie;", query.match().toString());

        query = Graql.match(var("x").isaX("movie")).insert(var().id("123").isa("movie"));
        assertEquals("match $x isa! movie;", query.match().toString());
    }

    @Test
    public void testInsertQueryGetVars() {
        GraqlInsert query = Graql.insert(var().id("123").isa("movie"), var().id("123").val("Hi"));
        // Should not merge variables
        assertEquals(2, query.statements().size());
    }

    @Test
    public void testDeleteQueryPattern() {
        GraqlDelete query = Graql.match(var("x").isa("movie")).delete("x");
        assertEquals("match $x isa movie;", query.match().toString());

        query = Graql.match(var("x").isaX("movie")).delete("x");
        assertEquals("match $x isa! movie;", query.match().toString());
    }
}
