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
import ai.grakn.graql.Order;
import ai.grakn.graql.VarName;
import ai.grakn.test.GraphContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.ErrorMessage.NEGATIVE_OFFSET;
import static ai.grakn.util.ErrorMessage.NON_POSITIVE_LIMIT;
import static ai.grakn.util.ErrorMessage.VARIABLE_NOT_IN_QUERY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MatchQueryTest {

    private GraknGraph graph = rule.graph();

    @ClassRule
    public static final GraphContext rule = GraphContext.preLoad(MovieGraph.get());

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
    }

    @Test
    public void whenQueryIsLimitedToANegativeNumber_Throw() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(NON_POSITIVE_LIMIT.getMessage(Long.MIN_VALUE));
        graph.graql().match(var()).limit(Long.MIN_VALUE);
    }

    @Test
    public void whenQueryIsLimitedToZero_Throw() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(NON_POSITIVE_LIMIT.getMessage(0L));
        graph.graql().match(var()).limit(0L);
    }

    @Test
    public void whenQueryIsOffsetByANegativeNumber_Throw() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(NEGATIVE_OFFSET.getMessage(Long.MIN_VALUE));
        graph.graql().match(var()).offset(Long.MIN_VALUE);
    }

    @Test
    public void testDistinctEmpty() {
        Set<Concept> result2 = graph.graql().match(
                var("x").isa("movie").has("title", var("y")),
                var("y").has("name", "xxx")).select("y").distinct().execute()
                .stream()
                .map(stringConceptMap -> stringConceptMap.get("x"))
                .collect(Collectors.toSet());
        assertTrue(result2.isEmpty());
    }

    @Test
    public void testDistinctTuple() {
        int size = graph.graql().match(var("x").isa("genre")).execute().size();
        size *= size;

        List<Map<String, Concept>> result1 = graph.graql().match(
                var("x").isa("genre"),
                var("x").isa("genre"),
                var("x").isa("genre"),
                var("y").isa("genre")).distinct().execute();
        assertEquals(size, result1.size());

        List<Map<String, Concept>> result2 = graph.graql().match(
                var().isa("genre"),
                var().isa("genre"),
                var().isa("genre"),
                var().isa("genre"),
                var().isa("genre")).distinct().execute();
        assertEquals(1, result2.size());

        List<Map<String, Concept>> result3 = graph.graql().match(
                var("x").isa("genre"),
                var("y").isa("genre")).distinct().execute();
        assertEquals(size, result3.size());

        List<Map<String, Concept>> result4 = graph.graql().match(
                var().isa("genre"),
                var("x").isa("genre"),
                var("y").isa("genre")).distinct().execute();
        assertEquals(size, result4.size());
    }

    @Test
    public void whenSelectingVarNotInQuery_Throw() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(VARIABLE_NOT_IN_QUERY.getMessage(VarName.of("x")));
        graph.graql().match(var()).select("x").execute();
    }

    @Test(expected = Exception.class)
    public void testVarNameEmptySet() {
        graph.graql().match(var()).select(Collections.EMPTY_SET).execute();
    }

    @Test(expected = Exception.class)
    public void testVarNameNullSet() {
        graph.graql().match(var()).select((Set<VarName>) null).execute();
    }

    @Test(expected = Exception.class)
    public void testVarNameNullString() {
        graph.graql().match(var()).select((String) null).execute();
    }

    @Test(expected = Exception.class)
    public void testOrderBy1() {
        graph.graql().match(var().isa("movie")).orderBy((String) null, Order.desc).execute();
    }

    @Test(expected = Exception.class)
    public void testOrderBy2() {
        graph.graql().match(var().isa("movie")).orderBy((VarName) null, Order.desc).execute();
    }

    @Test(expected = Exception.class)
    public void testOrderBy3() {
        graph.graql().match(var("x").isa("movie")).orderBy((String) null, Order.desc).execute();
    }

    @Test(expected = Exception.class)
    public void testOrderBy4() {
        graph.graql().match(var("x").isa("movie")).orderBy((VarName) null, Order.desc).execute();
    }

    @Test(expected = Exception.class)
    public void testOrderBy5() {
        graph.graql().match(var("x").isa("movie")).orderBy("y", Order.asc).execute();
    }

    @Test(expected = Exception.class)
    public void testOrderBy6() {
        graph.graql().match(var("x").isa("movie")).orderBy("x", null).execute();
    }

    @Test(expected = Exception.class) //TODO: error message should be more specific
    public void testOrderBy7() {
        graph.graql().match(var("x").isa("movie"),
                var().rel("x").rel("y")).orderBy("y", Order.asc).execute();
    }

    @Test(expected = Exception.class)
    public void testOrderBy8() {
        graph.graql().match(var("x").isa("movie")).orderBy("x", Order.asc).execute();
    }

}