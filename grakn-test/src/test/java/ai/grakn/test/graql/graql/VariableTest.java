/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.graql.graql;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graphs.MovieGraph;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.test.GraphContext;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VariableTest {

    private GraknGraph graph = rule.graph();

    @ClassRule
    public static final GraphContext rule = GraphContext.preLoad(MovieGraph.get());

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
    }

    @Test
    public void testVarName() {
        var("123");
        var("___");
        var("---");
        var("xxx");
        var("_1");
        var("1a");
        var("-1");
    }

    @Ignore
    @Test
    //TODO: FIX THIS TEST
    public void testVarNameInvalid() {
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
        Var var1;
        Var var2;

        var1 = var();
        var2 = var();
        assertTrue(var1.equals(var1));
        assertTrue(var1.equals(var2));

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
        Set<Var> varSet1 = Sets.newHashSet(
                var("x").isa("movie"),
                var("y1").isa("genre").has("name", "crime"),
                var().isa("has-genre").rel(var("x")).rel(var("y1")));
        Set<Concept> resultSet1 = graph.graql().match(varSet1).select("x").execute()
                .stream()
                .map(stringConceptMap -> stringConceptMap.get("x"))
                .collect(Collectors.toSet());
        assertFalse(resultSet1.isEmpty());

        Set<Var> varSet11 = Sets.newHashSet(var("x"));
        varSet11.addAll(varSet1);
        Set<Concept> resultSet11 = graph.graql().match(varSet11).select("x").execute()
                .stream()
                .map(stringConceptMap -> stringConceptMap.get("x"))
                .collect(Collectors.toSet());
        assertEquals(resultSet11, resultSet1);

        varSet11.add(var("z"));
        resultSet11 = graph.graql().match(varSet11).execute()
                .stream()
                .map(stringConceptMap -> stringConceptMap.get("x"))
                .collect(Collectors.toSet());
        assertEquals(resultSet11, resultSet1);

        Set<Var> varSet2 = Sets.newHashSet(
                var("x").isa("movie"),
                var("y2").isa("person").has("name", "Marlon Brando"),
                var().isa("has-cast").rel(var("x")).rel(var("y2")));
        Set<Concept> resultSet2 = graph.graql().match(varSet2).select("x").execute()
                .stream()
                .map(stringConceptMap -> stringConceptMap.get("x"))
                .collect(Collectors.toSet());
        assertFalse(resultSet1.isEmpty());

        Set<Var> varSet3 = Sets.newHashSet(
                var("x").isa("movie"),
                var("y").isa("genre").has("name", "crime"),
                var().isa("has-genre").rel(var("x")).rel(var("y")),
                var("y2").isa("person").has("name", "Marlon Brando"),
                var().isa("has-cast").rel(var("x")).rel(var("y2")));
        Set<Concept> conj = graph.graql().match(varSet3).select("x").execute()
                .stream()
                .map(stringConceptMap -> stringConceptMap.get("x"))
                .collect(Collectors.toSet());
        assertFalse(conj.isEmpty());

        resultSet2.retainAll(resultSet1);

        assertEquals(resultSet2, conj);

        conj = graph.graql().match(and(varSet3)).execute().stream()
                .map(stringConceptMap -> stringConceptMap.get("x"))
                .collect(Collectors.toSet());

        assertEquals(resultSet2, conj);
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