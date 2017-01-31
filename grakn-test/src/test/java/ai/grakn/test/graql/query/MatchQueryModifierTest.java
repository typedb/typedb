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

import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.GraphContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graphs.MovieGraph.apocalypseNow;
import static ai.grakn.graphs.MovieGraph.get;
import static ai.grakn.graphs.MovieGraph.godfather;
import static ai.grakn.graphs.MovieGraph.heat;
import static ai.grakn.graphs.MovieGraph.hocusPocus;
import static ai.grakn.graphs.MovieGraph.kermitTheFrog;
import static ai.grakn.graphs.MovieGraph.missPiggy;
import static ai.grakn.graphs.MovieGraph.spy;
import static ai.grakn.graphs.MovieGraph.theMuppets;
import static ai.grakn.graql.Graql.neq;
import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.Order.asc;
import static ai.grakn.graql.Order.desc;
import static ai.grakn.test.graql.query.QueryUtil.containsAllMovies;
import static ai.grakn.test.graql.query.QueryUtil.variable;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class MatchQueryModifierTest {

    private QueryBuilder qb;

    @ClassRule
    public static final GraphContext rule = GraphContext.preLoad(get());

    @Before
    public void setUp() {
        qb = rule.graph().graql();
    }

    @Test
    public void testOffsetQuery() {
        MatchQuery query = qb.match(var("x").isa("movie").has("name", var("n"))).orderBy("n", desc).offset(4);

        assertResultsOrderedByValue(query, "n", false);
    }

    @Test
    public void testLimitQuery() {
        MatchQuery query = qb.match(var("x").isa("movie").has("title", var("t"))).orderBy("t", asc).offset(1).limit(3);

        assertResultsOrderedByValue(query, "t", true);
        assertEquals(3, query.stream().count());
    }

    @Test
    public void testOrPatternOrderByResource() {
        MatchQuery query = qb.match(
                var("x").isa("movie").has("tmdb-vote-count", var("v")),
                var().rel("x").rel("y"),
                or(
                        var("y").isa("person").has("name", "Marlon Brando"),
                        var("y").isa("genre").has("name", "crime")
                )
        ).orderBy("v", desc);

        assertThat(query, variable("x", contains(godfather, godfather, apocalypseNow)));
    }

    @Test
    public void testOrPatternOrderByUnselected() {
        MatchQuery query = qb.match(
                var("x").isa("movie"),
                var().rel("x").rel("y"),
                or(
                        var("y").isa("person"),
                        var("y").isa("genre").value(neq("crime"))
                ),
                var("y").has("name", var("n"))
        ).orderBy("n").offset(4).limit(8).select("x");

        assertThat(query, variable("x", containsInAnyOrder(hocusPocus, spy, theMuppets, godfather, apocalypseNow)));
    }

    @Test
    public void testValueOrderedQuery() {
        MatchQuery query = qb.match(var("the-movie").isa("movie").has("title", var("n"))).orderBy("n", desc);

        assertResultsOrderedByValue(query, "n", false);

        // Make sure all results are included
        assertThat(query, variable("x", containsAllMovies()));
    }

    @Test
    public void testVoteCountOrderedQuery() {
        MatchQuery query = qb.match(var("z").isa("movie").has("tmdb-vote-count", var("v"))).orderBy("v", desc);

        // Make sure movies are in the correct order
        assertThat(query, variable("x", contains(godfather, hocusPocus, apocalypseNow, theMuppets)));
    }

    @Test
    public void testOrPatternDistinct() {
        MatchQuery query = qb.match(
                var("x").isa("movie").has("title", var("t")),
                var().rel("x").rel("y"),
                or(
                        var("y").isa("genre").has("name", "crime"),
                        var("y").isa("person").has("name", "Marlon Brando")
                )
        ).orderBy("t", desc).select("x").distinct();

        assertThat(query, variable("x", contains(heat, godfather, apocalypseNow)));
    }

    @Test
    public void testNondistinctQuery() {
        MatchQuery query = qb.match(
                var("x").isa("person"),
                var("y").has("title", "The Muppets"),
                var().rel("x").rel("y")
        ).select("x");

        assertThat(query, variable("x", containsInAnyOrder(kermitTheFrog, kermitTheFrog, missPiggy, missPiggy)));
    }

    @Test
    public void testDistinctQuery() {
        MatchQuery query = qb.match(
                var("x").isa("person"),
                var("y").has("title", "The Muppets"),
                var().rel("x").rel("y")
        ).distinct().select("x");

        assertThat(query, variable("x", containsInAnyOrder(kermitTheFrog, missPiggy)));
    }

    private <T extends Comparable<T>> void assertResultsOrderedByValue(MatchQuery query, String var, boolean asc) {
        Stream<T> values = query.stream().map(result -> result.get(var).<T>asResource().getValue());
        assertResultsOrdered(values, asc);
    }

    private <T extends Comparable<T>> void assertResultsOrdered(Stream<T> results, boolean asc) {
        T previous = null;

        for (T result : results.collect(Collectors.toList())) {
            if (previous != null) {
                if (asc) {
                    assertTrue(result.compareTo(previous) >= 0);
                } else {
                    assertTrue(result.compareTo(previous) <= 0);
                }
            }
            previous = result;
        }
    }
}
