/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query.pattern;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.VarPattern;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import com.google.common.collect.Sets;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.neq;
import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.GraqlTestUtil.assertExists;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PatternTest {

    private GraknTx tx = rule.tx();

    @ClassRule
    public static final SampleKBContext rule = MovieKB.context();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testVarPattern() {
        Pattern x = var("x");

        assertTrue(x.admin().isVarPattern());
        assertFalse(x.admin().isDisjunction());
        assertFalse(x.admin().isConjunction());

        assertEquals(x.admin(), x.admin().asVarPattern());
    }

    @Test
    public void testSimpleDisjunction() {
        Pattern disjunction = or();

        assertFalse(disjunction.admin().isVarPattern());
        assertTrue(disjunction.admin().isDisjunction());
        assertFalse(disjunction.admin().isConjunction());

        //noinspection AssertEqualsBetweenInconvertibleTypes
        assertEquals(disjunction.admin(), disjunction.admin().asDisjunction());
    }

    @Test
    public void testSimpleConjunction() {
        Pattern conjunction = and();

        assertFalse(conjunction.admin().isVarPattern());
        assertFalse(conjunction.admin().isDisjunction());
        assertTrue(conjunction.admin().isConjunction());

        //noinspection AssertEqualsBetweenInconvertibleTypes
        assertEquals(conjunction.admin(), conjunction.admin().asConjunction());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testConjunctionAsVar() {
        //noinspection ResultOfMethodCallIgnored
        Graql.and(var("x").isa("movie"), var("x").isa("person")).admin().asVarPattern();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDisjunctionAsConjunction() {
        //noinspection ResultOfMethodCallIgnored
        Graql.or(var("x").isa("movie"), var("x").isa("person")).admin().asConjunction();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testVarAsDisjunction() {
        //noinspection ResultOfMethodCallIgnored
        var("x").isa("movie").admin().asDisjunction();
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
        VarPattern var1;
        VarPattern var2;

        var1 = var();
        var2 = var();
        assertTrue(var1.equals(var1));
        assertFalse(var1.equals(var2));

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
        Set<VarPattern> varSet1 = Sets.newHashSet(
                var("x").isa("movie"),
                var("y1").isa("genre").has("name", "crime"),
                var().isa("has-genre").rel(var("x")).rel(var("y1")));
        Set<Concept> resultSet1 = tx.graql().match(varSet1).get("x").collect(Collectors.toSet());
        assertFalse(resultSet1.isEmpty());

        Set<VarPattern> varSet11 = Sets.newHashSet(var("x"));
        varSet11.addAll(varSet1);
        Set<Concept> resultSet11 = tx.graql().match(varSet11).get("x").collect(Collectors.toSet());
        assertEquals(resultSet11, resultSet1);

        varSet11.add(var("z"));
        resultSet11 = tx.graql().match(varSet11).get("x").collect(Collectors.toSet());
        assertEquals(resultSet11, resultSet1);

        Set<VarPattern> varSet2 = Sets.newHashSet(
                var("x").isa("movie"),
                var("y2").isa("person").has("name", "Marlon Brando"),
                var().isa("has-cast").rel(var("x")).rel(var("y2")));
        Set<Concept> resultSet2 = tx.graql().match(varSet2).get("x").collect(Collectors.toSet());
        assertFalse(resultSet1.isEmpty());

        Set<VarPattern> varSet3 = Sets.newHashSet(
                var("x").isa("movie"),
                var("y").isa("genre").has("name", "crime"),
                var().isa("has-genre").rel(var("x")).rel(var("y")),
                var("y2").isa("person").has("name", "Marlon Brando"),
                var().isa("has-cast").rel(var("x")).rel(var("y2")));
        Set<Concept> conj = tx.graql().match(varSet3).get("x").collect(Collectors.toSet());
        assertFalse(conj.isEmpty());

        resultSet2.retainAll(resultSet1);
        assertEquals(resultSet2, conj);

        conj = tx.graql().match(and(varSet3)).get("x").collect(Collectors.toSet());
        assertEquals(resultSet2, conj);

        conj = tx.graql().match(or(var("x"), var("x"))).get("x").collect(Collectors.toSet());
        assertTrue(conj.size() > 1);
    }

    @Test(expected = Exception.class)
    public void whenConjunctionPassedNull_Throw() {
        Set<VarPattern> varSet = null;
        //noinspection ResultOfMethodCallIgnored,ConstantConditions
        and(varSet);
    }

    @Test(expected = Exception.class)
    public void whenConjunctionPassedVarAndNull_Throw() {
        VarPattern var = null;
        //noinspection ResultOfMethodCallIgnored,ConstantConditions
        and(var(), var);
    }

    @Test
    public void testDisjunction() {
        Set<VarPattern> varSet1 = Sets.newHashSet(
                var("x").isa("movie"),
                var("y1").isa("genre").has("name", "crime"),
                var().isa("has-genre").rel(var("x")).rel(var("y1")));
        Set<Concept> resultSet1 = tx.graql().match(varSet1).get("x").collect(Collectors.toSet());
        assertFalse(resultSet1.isEmpty());

        Set<VarPattern> varSet2 = Sets.newHashSet(
                var("x").isa("movie"),
                var("y2").isa("person").has("name", "Marlon Brando"),
                var().isa("has-cast").rel(var("x")).rel(var("y2")));
        Set<Concept> resultSet2 = tx.graql().match(varSet2).get("x").collect(Collectors.toSet());
        assertFalse(resultSet1.isEmpty());

        Set<Pattern> varSet3 = Sets.newHashSet(
                var("x").isa("movie"),
                or(var("y").isa("genre").has("name", "crime"),
                        var("y").isa("person").has("name", "Marlon Brando")),
                var().rel(var("x")).rel(var("y")));
        Set<Concept> conj = tx.graql().match(varSet3).get("x").collect(Collectors.toSet());
        assertFalse(conj.isEmpty());

        resultSet2.addAll(resultSet1);
        assertEquals(resultSet2, conj);

        conj = tx.graql().match(or(var("x"), var("x"))).get("x").collect(Collectors.toSet());
        assertTrue(conj.size() > 1);
    }

    @Test(expected = Exception.class)
    public void whenDisjunctionPassedNull_Throw() {
        Set<VarPattern> varSet = null;
        //noinspection ResultOfMethodCallIgnored,ConstantConditions
        or(varSet);
    }

    @Test(expected = Exception.class)
    public void whenDisjunctionPassedVarAndNull_Throw() {
        VarPattern var = null;
        //noinspection ResultOfMethodCallIgnored,ConstantConditions
        or(var(), var);
    }

    @Test
    public void testNegation() {
        assertExists(tx.graql(), var().isa("movie").has("title", "Godfather"));
        Set<Concept> result1 = tx.graql().match(
                var("x").isa("movie").has("title", var("y")),
                var("y").val(neq("Godfather"))).get("x").collect(Collectors.toSet());
        assertFalse(result1.isEmpty());

        Set<Concept> result2 = tx.graql().match(
                var("x").isa("movie").has("title", var("y")),
                var("y")).get("x").collect(Collectors.toSet());
        assertFalse(result2.isEmpty());

        result2.removeAll(result1);
        assertEquals(1, result2.size());
    }

    @Test(expected = Exception.class)
    public void whenNegationPassedNull_Throw() {
        VarPattern var = null;
        //noinspection ConstantConditions,ResultOfMethodCallIgnored
        neq(var);
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
