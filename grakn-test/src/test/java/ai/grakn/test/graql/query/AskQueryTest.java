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

package ai.grakn.test.graql.query;

import ai.grakn.graphs.MovieGraph;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.GraphContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AskQueryTest {

    @ClassRule
    public static final GraphContext graph = GraphContext.preLoad(MovieGraph.get());

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private QueryBuilder qb;

    @Before
    public void setUp() {
        qb = graph.graph().graql();
    }

    @Test
    public void testPositiveQuery() {
        assertTrue(qb.match(var().isa("movie").has("tmdb-vote-count", 1000)).ask().execute());
    }

    @Test
    public void testNegativeQuery() {
        assertFalse(qb.match(var("y").isa("award")).ask().execute());
    }

    @Test
    public void testNegativeQueryShortcutEdge() {
        assertFalse(qb.match(
                var().rel("x").rel("y").isa("directed-by"),
                var("x").val("Apocalypse Now"),
                var("y").val("Martin Sheen")
        ).ask().execute());
    }

    @Test
    public void testAskNoVariables() {
        assertTrue(qb.match(label("person").plays("actor")).ask().execute());
    }
}
