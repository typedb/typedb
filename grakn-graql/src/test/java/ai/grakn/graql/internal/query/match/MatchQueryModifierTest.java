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

package ai.grakn.graql.internal.query.match;

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.matcher.MovieMatchers;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.neq;
import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.Order.asc;
import static ai.grakn.graql.Order.desc;
import static ai.grakn.matcher.GraknMatchers.variable;
import static ai.grakn.matcher.MovieMatchers.containsAllMovies;
import static ai.grakn.util.ErrorMessage.VARIABLE_NOT_IN_QUERY;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class MatchQueryModifierTest {

    private QueryBuilder qb;

    private static final Var x = Graql.var("x");
    private static final Var y = Graql.var("y");
    private static final Var n = Graql.var("n");

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final SampleKBContext rule = SampleKBContext.preLoad(MovieKB.get());

    @Before
    public void setUp() {
        qb = rule.tx().graql();
    }

    @Test
    public void testOffsetQuery() {
        MatchQuery query = qb.match(x.isa("movie").has("name", n)).orderBy(n, desc).offset(4);

        assertResultsOrderedByValue(query, n, false);
    }

    @Test
    public void testLimitQuery() {
        Var t = var("t");
        MatchQuery query = qb.match(x.isa("movie").has("title", t)).orderBy(t, asc).offset(1).limit(3);

        assertResultsOrderedByValue(query, t, true);
        assertEquals(3, query.stream().count());
    }

    @Test
    public void testOrPatternOrderByResource() {
        MatchQuery query = qb.match(
                x.isa("movie").has("tmdb-vote-count", var("v")),
                var().rel(x).rel(y),
                or(
                        y.isa("person").has("name", "Marlon Brando"),
                        y.isa("genre").has("name", "crime")
                )
        ).orderBy("v", desc);

        assertThat(query, variable(x, contains(MovieMatchers.godfather, MovieMatchers.godfather, MovieMatchers.apocalypseNow)));
    }

    @Test
    public void testOrPatternOrderByUnselected() {
        MatchQuery query = qb.match(
                x.isa("movie"),
                var().rel(x).rel(y),
                or(
                        y.isa("person"),
                        y.isa("genre").val(neq("crime"))
                ),
                y.has("name", n)
        ).orderBy(n).offset(4).limit(8).select(x);

        assertThat(query, variable(x, containsInAnyOrder(
                MovieMatchers.hocusPocus, MovieMatchers.spy, MovieMatchers.spy, MovieMatchers.theMuppets, MovieMatchers.theMuppets, MovieMatchers.godfather, MovieMatchers.apocalypseNow, MovieMatchers.apocalypseNow
        )));
    }

    @Test
    public void testValueOrderedQuery() {
        Var theMovie = var("the-movie");
        MatchQuery query = qb.match(theMovie.isa("movie").has("title", n)).orderBy(n, desc);

        assertResultsOrderedByValue(query, n, false);

        // Make sure all results are included
        assertThat(query, variable(theMovie, containsAllMovies));
    }

    @Test
    public void testVoteCountOrderedQuery() {
        Var z = var("z");
        MatchQuery query = qb.match(z.isa("movie").has("tmdb-vote-count", var("v"))).orderBy("v", desc);

        // Make sure movies are in the correct order
        assertThat(query, variable(z, contains(MovieMatchers.godfather, MovieMatchers.hocusPocus, MovieMatchers.apocalypseNow, MovieMatchers.theMuppets, MovieMatchers.chineseCoffee)));
    }

    @Test
    public void testOrPatternDistinct() {
        MatchQuery query = qb.match(
                x.isa("movie").has("title", var("t")),
                var().rel(x).rel(y),
                or(
                        y.isa("genre").has("name", "crime"),
                        y.isa("person").has("name", "Marlon Brando")
                )
        ).orderBy("t", desc).select(x).distinct();

        assertThat(query, variable(x, contains(MovieMatchers.heat, MovieMatchers.godfather, MovieMatchers.apocalypseNow)));
    }

    @Test
    public void testNondistinctQuery() {
        MatchQuery query = qb.match(
                x.isa("person"),
                y.has("title", "The Muppets"),
                var().rel(x).rel(y)
        ).select(x);

        assertThat(query, variable(x, containsInAnyOrder(MovieMatchers.kermitTheFrog, MovieMatchers.kermitTheFrog, MovieMatchers.missPiggy, MovieMatchers.missPiggy)));
    }

    @Test
    public void testDistinctQuery() {
        MatchQuery query = qb.match(
                x.isa("person"),
                y.has("title", "The Muppets"),
                var().rel(x).rel(y)
        ).distinct().select(x);

        assertThat(query, variable(x, containsInAnyOrder(MovieMatchers.kermitTheFrog, MovieMatchers.missPiggy)));
    }

    @Test
    public void testSelectVariableNotInQuery() {
        MatchQuery query = qb.match(x.isa("movie"));

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(VARIABLE_NOT_IN_QUERY.getMessage(y));

        //noinspection ResultOfMethodCallIgnored
        query.select(y);
    }

    private <T extends Comparable<T>> void assertResultsOrderedByValue(MatchQuery query, Var var, boolean asc) {
        Stream<T> values = query.stream().map(result -> result.get(var).<T>asAttribute().getValue());
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
