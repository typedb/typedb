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

package grakn.core.graql.query.pattern;

import com.google.common.collect.Sets;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.graql.query.Graql;
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

import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static grakn.core.graql.query.Graql.and;
import static grakn.core.graql.query.Graql.not;
import static grakn.core.graql.query.Graql.or;
import static grakn.core.graql.query.Graql.var;
import static grakn.core.util.GraqlTestUtil.assertExists;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class PatternIT {

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

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testValidVarNames() {
        var("123");
        var("___");
        var("---");
        var("xxx");
        var("_1");
        var("1a");
        var("-1");
    }

    @Test
    public void whenCreatingAVarWithAnInvalidName_Throw() {
        assertExceptionThrown(Graql::var, "");
        assertExceptionThrown(Graql::var, " ");
        assertExceptionThrown(Graql::var, "!!!");
        assertExceptionThrown(Graql::var, "a b");
        assertExceptionThrown(Graql::var, "");
        assertExceptionThrown(Graql::var, "\"");
        assertExceptionThrown(Graql::var, "\"\"");
        assertExceptionThrown(Graql::var, "'");
        assertExceptionThrown(Graql::var, "''");
    }

    @Test
    public void testVarEquals() {
        Statement var1;
        Statement var2;

        var1 = var();
        var2 = var();
        assertTrue(var1.var().equals(var1.var()));
        assertFalse(var1.var().equals(var2.var()));

        var1 = var("x");
        var2 = var("y");
        assertTrue(var1.equals(var1));
        assertFalse(var1.equals(var2));

        var1 = var("x").isa("movie");
        var2 = var("x").isa("movie");
        assertTrue(var1.equals(var2));

        var1 = var("x").isa("movie").has("title", "abc");
        var2 = var("x").has("title", "abc").isa("movie");
        assertTrue(var1.equals(var2));
    }

    @Test
    public void testConjunction() {
        Set<Statement> varSet1 = Sets.newHashSet(
                var("x").isa("movie"),
                var("y1").isa("genre").has("name", "crime"),
                var().isa("has-genre").rel(var("x")).rel(var("y1")));
        Set<Concept> resultSet1 = tx.stream(Graql.match(varSet1).get("x"))
                .map(ans -> ans.get("x")).collect(Collectors.toSet());
        assertFalse(resultSet1.isEmpty());

        Set<Statement> varSet11 = Sets.newHashSet(var("x"));
        varSet11.addAll(varSet1);
        Set<Concept> resultSet11 = tx.stream(Graql.match(varSet11).get("x"))
                .map(ans -> ans.get("x")).collect(Collectors.toSet());
        assertEquals(resultSet11, resultSet1);

        varSet11.add(var("z"));
        resultSet11 = tx.stream(Graql.match(varSet11).get("x"))
                .map(ans -> ans.get("x")).collect(Collectors.toSet());
        assertEquals(resultSet11, resultSet1);

        Set<Statement> varSet2 = Sets.newHashSet(
                var("x").isa("movie"),
                var("y2").isa("person").has("name", "Marlon Brando"),
                var().isa("has-cast").rel(var("x")).rel(var("y2")));
        Set<Concept> resultSet2 = tx.stream(Graql.match(varSet2).get("x"))
                .map(ans -> ans.get("x")).collect(Collectors.toSet());
        assertFalse(resultSet1.isEmpty());

        Set<Statement> varSet3 = Sets.newHashSet(
                var("x").isa("movie"),
                var("y").isa("genre").has("name", "crime"),
                var().isa("has-genre").rel(var("x")).rel(var("y")),
                var("y2").isa("person").has("name", "Marlon Brando"),
                var().isa("has-cast").rel(var("x")).rel(var("y2")));
        Set<Concept> conj = tx.stream(Graql.match(varSet3).get("x"))
                .map(ans -> ans.get("x")).collect(Collectors.toSet());
        assertFalse(conj.isEmpty());

        resultSet2.retainAll(resultSet1);
        assertEquals(resultSet2, conj);

        conj = tx.stream(Graql.match(and(varSet3)).get("x"))
                .map(ans -> ans.get("x")).collect(Collectors.toSet());
        assertEquals(resultSet2, conj);

        conj = tx.stream(Graql.match(or(var("x"), var("x"))).get("x"))
                .map(ans -> ans.get("x")).collect(Collectors.toSet());
        assertTrue(conj.size() > 1);
    }

    @Test
    public void whenConjunctionPassedNull_Throw() {
        exception.expect(Exception.class);
        Set<Statement> varSet = null;
        //noinspection ResultOfMethodCallIgnored,ConstantConditions
        and(varSet);
    }

    @Test
    public void whenConjunctionPassedVarAndNull_Throw() {
        exception.expect(Exception.class);
        Statement var = null;
        //noinspection ResultOfMethodCallIgnored,ConstantConditions
        and(var(), var);
    }

    @Test
    public void testDisjunction() {
        Set<Statement> varSet1 = Sets.newHashSet(
                var("x").isa("movie"),
                var("y1").isa("genre").has("name", "crime"),
                var().isa("has-genre").rel(var("x")).rel(var("y1")));
        Set<Concept> resultSet1 = tx.stream(Graql.match(varSet1).get("x"))
                .map(ans -> ans.get("x")).collect(Collectors.toSet());
        assertFalse(resultSet1.isEmpty());

        Set<Statement> varSet2 = Sets.newHashSet(
                var("x").isa("movie"),
                var("y2").isa("person").has("name", "Marlon Brando"),
                var().isa("has-cast").rel(var("x")).rel(var("y2")));
        Set<Concept> resultSet2 = tx.stream(Graql.match(varSet2).get("x"))
                .map(ans -> ans.get("x")).collect(Collectors.toSet());
        assertFalse(resultSet1.isEmpty());

        Set<Pattern> varSet3 = Sets.newHashSet(
                var("x").isa("movie"),
                or(var("y").isa("genre").has("name", "crime"),
                        var("y").isa("person").has("name", "Marlon Brando")),
                var().rel(var("x")).rel(var("y")));
        Set<Concept> conj = tx.stream(Graql.match(varSet3).get("x"))
                .map(ans -> ans.get("x")).collect(Collectors.toSet());
        assertFalse(conj.isEmpty());

        resultSet2.addAll(resultSet1);
        assertEquals(resultSet2, conj);

        conj = tx.stream(Graql.match(or(var("x"), var("x"))).get("x"))
                .map(ans -> ans.get("x")).collect(Collectors.toSet());
        assertTrue(conj.size() > 1);
    }

    @Test
    public void whenDisjunctionPassedNull_Throw() {
        exception.expect(Exception.class);
        Set<Statement> varSet = null;
        //noinspection ResultOfMethodCallIgnored,ConstantConditions
        or(varSet);
    }

    @Test
    public void whenDisjunctionPassedVarAndNull_Throw() {
        exception.expect(Exception.class);
        Statement var = null;
        //noinspection ResultOfMethodCallIgnored,ConstantConditions
        or(var(), var);
    }

    @Test
    public void whenNegationPassedNull_Throw() {
        exception.expect(Exception.class);
        Statement var = null;
        //noinspection ResultOfMethodCallIgnored,ConstantConditions
        not(var);
    }

    @Test
    public void testNeq() {
        assertExists(tx, var().isa("movie").has("title", "Godfather"));
        Set<Concept> result1 = tx.stream(Graql.match(
                var("x").isa("movie").has("title", var("y")),
                var("y").neq("Godfather")).get("x"))
                .map(ans -> ans.get("x")).collect(Collectors.toSet());
        assertFalse(result1.isEmpty());

        Set<Concept> result2 = tx.stream(Graql.match(
                var("x").isa("movie").has("title", var("y")),
                var("y")).get("x"))
                .map(ans -> ans.get("x")).collect(Collectors.toSet());
        assertFalse(result2.isEmpty());

        result2.removeAll(result1);
        assertEquals(1, result2.size());
    }

    private void assertExceptionThrown(Consumer<String> consumer, String varName) {
        boolean exceptionThrown = false;
        try {
            consumer.accept(varName);
        } catch (Exception e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }
}
