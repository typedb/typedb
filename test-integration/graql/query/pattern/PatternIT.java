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

package grakn.core.graql.query.pattern;

import com.google.common.collect.Sets;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static grakn.core.util.GraqlTestUtil.assertExists;
import static graql.lang.Graql.and;
import static graql.lang.Graql.not;
import static graql.lang.Graql.or;
import static graql.lang.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class PatternIT {

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

        Set<Statement> varSet11 = Sets.newHashSet(var("x").isa("thing"));
        varSet11.addAll(varSet1);
        Set<Concept> resultSet11 = tx.stream(Graql.match(varSet11).get("x"))
                .map(ans -> ans.get("x")).collect(Collectors.toSet());
        assertEquals(resultSet11, resultSet1);

        varSet11.add(var("z").isa("thing"));
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

        conj = tx.stream(Graql.match(or(var("x").isa("thing"), var("x").isa("thing"))).get("x"))
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

        conj = tx.stream(Graql.match(or(var("x").isa("thing"), var("x").isa("thing"))).get("x"))
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
    public void whenMatchingWithValueInequality_resultsAreFilteredCorrectly() {
        assertExists(tx, var("x").isa("movie").has("title", "Godfather"));
        Set<Concept> allMoviesWithoutGodfatherMovies = tx.stream(Graql.match(
                var("x").isa("movie").has("title", var("y")),
                var("y").neq("Godfather")).get("x"))
                .map(ans -> ans.get("x"))
                .collect(Collectors.toSet());
        assertFalse(allMoviesWithoutGodfatherMovies.isEmpty());

        Set<Concept> allMovies = tx.stream(Graql.match(
                var("x").isa("movie").has("title", var("y"))).get("x"))
                .map(ans -> ans.get("x"))
                .collect(Collectors.toSet());
        assertFalse(allMovies.isEmpty());

        Set<Concept> godfatherMovies = tx.stream(Graql.match(
                var("x").isa("movie").has("title", "Godfather")).get("x"))
                .map(ans -> ans.get("x"))
                .collect(Collectors.toSet());
        assertFalse(godfatherMovies.isEmpty());

        assertEquals(Sets.difference(allMovies, godfatherMovies), allMoviesWithoutGodfatherMovies);
    }


    @Test
    public void whenStatementDoesntHaveProperties_weThrow() {
        // empty `match $x; get;` not allowed
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage("Require statement to have at least one property");
        List<ConceptMap> answers = tx.execute(Graql.match(var("x")).get());
    }

    @Test
    public void whenComputingValueComparisons_weDontGetShardErrors() {
        // this case can throw [SHARD] errors if not handled correctly - unbound variable may cause issues
        tx.execute(Graql.match(var("x").neq(100)).get());
    }

    @Test
    public void whenValueComparisonIsUnbound_weThrow() {
        // value comparison
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage("Variables used in comparisons cannot be unbound");
        List<ConceptMap> answers = tx.execute(Graql.match(var("x").neq(var("y"))).get());

        // concept comparison
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage("Variables used in comparisons cannot be unbound");
        answers = tx.execute(Graql.match( var("y").not("x")).get());
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
