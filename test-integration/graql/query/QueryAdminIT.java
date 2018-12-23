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

import com.google.common.collect.Sets;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import static grakn.core.graql.query.pattern.Pattern.var;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class QueryAdminIT {

    @ClassRule
    public static GraknTestServer graknServer = new GraknTestServer();
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
    public void testDefaultGetSelectedNamesInQuery() {
        MatchClause match = Graql.match(var("x").isa(var("y")));

        assertEquals(Sets.newHashSet(var("x"), var("y")), match.getSelectedNames());
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
        InsertQuery query = Graql.insert(var().id("123").isa("movie"));
        assertNull(query.match());
    }

    @Test
    public void testInsertQueryWithMatch() {
        InsertQuery query = Graql.match(var("x").isa("movie")).insert(var().id("123").isa("movie"));
        assertEquals("match $x isa movie;", query.match().toString());

        query = Graql.match(var("x").isaExplicit("movie")).insert(var().id("123").isa("movie"));
        assertEquals("match $x isa! movie;", query.match().toString());
    }

    @Test
    public void testInsertQueryGetVars() {
        InsertQuery query = Graql.insert(var().id("123").isa("movie"), var().id("123").val("Hi"));
        // Should not merge variables
        assertEquals(2, query.statements().size());
    }

    @Test
    public void testDeleteQueryPattern() {
        DeleteQuery query = Graql.match(var("x").isa("movie")).delete("x");
        assertEquals("match $x isa movie;", query.match().toString());

        query = Graql.match(var("x").isaExplicit("movie")).delete("x");
        assertEquals("match $x isa! movie;", query.match().toString());
    }
}
