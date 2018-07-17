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

package ai.grakn.graql.internal.query.match;

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Match;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Streamable;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.ConceptMap;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import com.google.common.collect.ImmutableSet;
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
import static ai.grakn.matcher.MovieMatchers.apocalypseNow;
import static ai.grakn.matcher.MovieMatchers.chineseCoffee;
import static ai.grakn.matcher.MovieMatchers.containsAllMovies;
import static ai.grakn.matcher.MovieMatchers.godfather;
import static ai.grakn.matcher.MovieMatchers.heat;
import static ai.grakn.matcher.MovieMatchers.hocusPocus;
import static ai.grakn.matcher.MovieMatchers.theMuppets;
import static ai.grakn.util.ErrorMessage.VARIABLE_NOT_IN_QUERY;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class MatchModifierTest {

    private QueryBuilder qb;

    private static final Var x = Graql.var("x");
    private static final Var y = Graql.var("y");
    private static final Var n = Graql.var("n");

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final SampleKBContext rule = MovieKB.context();

    @Before
    public void setUp() {
        qb = rule.tx().graql();
    }

    @Test
    public void testOffsetQuery() {
        Match match = qb.match(x.isa("movie").has("name", n)).orderBy(n, desc).offset(4);

        assertResultsOrderedByValue(match, n, false);
    }

    @Test
    public void testLimitQuery() {
        Var t = var("t");
        Match match = qb.match(x.isa("movie").has("title", t)).orderBy(t, asc).offset(1).limit(3);

        assertResultsOrderedByValue(match, t, true);
        assertEquals(3, match.stream().count());
    }

    @Test
    public void testOrPatternOrderByResource() {
        Match match = qb.match(
                x.isa("movie").has("tmdb-vote-count", var("v")),
                var().rel(x).rel(y),
                or(
                        y.isa("person").has("name", "Marlon Brando"),
                        y.isa("genre").has("name", "crime")
                )
        ).orderBy("v", desc);

        assertThat(match, variable(x, contains(godfather, godfather, apocalypseNow)));
    }

    @Test
    public void testOrPatternOrderByUnselected() {
        Match match = qb.match(
                x.isa("movie"),
                var().rel(x).rel(y),
                or(
                        y.isa("person"),
                        y.isa("genre").val(neq("crime"))
                ),
                y.has("name", n)
        ).orderBy(n);

        assertResultsOrderedByValue(match, n, true);
    }

    @Test
    public void testValueOrderedQuery() {
        Var theMovie = var("the-movie");
        Match match = qb.match(theMovie.isa("movie").has("title", n)).orderBy(n, desc);

        assertResultsOrderedByValue(match, n, false);

        // Make sure all results are included
        assertThat(match, variable(theMovie, containsAllMovies));
    }

    @Test
    public void testVoteCountOrderedQuery() {
        Var z = var("z");
        Match match = qb.match(z.isa("movie").has("tmdb-vote-count", var("v"))).orderBy("v", desc);

        // Make sure movies are in the correct order
        assertThat(match, variable(z, contains(godfather, hocusPocus, apocalypseNow, theMuppets, chineseCoffee)));
    }

    @Test
    public void testOrPatternDistinct() {
        Match match = qb.match(
                x.isa("movie").has("title", var("t")),
                var().rel(x).rel(y),
                or(
                        y.isa("genre").has("name", "crime"),
                        y.isa("person").has("name", "Marlon Brando")
                )
        ).orderBy("t", desc);

        assertThat(match, variable(x, contains(heat, godfather, godfather, apocalypseNow)));
    }

    @Test
    public void whenGettingAVarNotInQuery_Throw() {
        Match match = qb.match(x.isa("movie"));

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(VARIABLE_NOT_IN_QUERY.getMessage(y));

        //noinspection ResultOfMethodCallIgnored
        match.get(ImmutableSet.of(y));
    }

    private <T extends Comparable<T>> void assertResultsOrderedByValue(
            Streamable<ConceptMap> streamable, Var var, boolean asc) {
        Stream<T> values = streamable.stream().map(result -> result.get(var).<T>asAttribute().value());
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
