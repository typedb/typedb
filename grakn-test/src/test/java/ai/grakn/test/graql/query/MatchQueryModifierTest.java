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

import ai.grakn.concept.Concept;
import ai.grakn.concept.ResourceType;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.AbstractMovieGraphTest;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.neq;
import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.Order.asc;
import static ai.grakn.graql.Order.desc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MatchQueryModifierTest extends AbstractMovieGraphTest {

    private QueryBuilder qb;

    @Before
    public void setUp() {
        qb = graph.graql();
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

        assertOrderedResultsMatch(query, "x", "movie", "Godfather", "Godfather", "Apocalypse Now");
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

        QueryUtil.assertResultsMatch(
                query, "x", "movie", graph.getResourceType("title"), "Hocus Pocus", "Spy", "The Muppets", "Godfather", "Apocalypse Now"
        );
    }

    @Test
    public void testValueOrderedQuery() {
        MatchQuery query = qb.match(var("the-movie").isa("movie").has("title", var("n"))).orderBy("n", desc);

        assertResultsOrderedByValue(query, "n", false);

        // Make sure all results are included
        QueryUtil.assertResultsMatch(query, "the-movie", "movie", graph.getResourceType("title"), QueryUtil.movies);
    }

    @Test
    public void testVoteCountOrderedQuery() {
        MatchQuery query = qb.match(var("z").isa("movie").has("tmdb-vote-count", var("v"))).orderBy("v", desc);

        // Make sure movies are in the correct order
        assertOrderedResultsMatch(query, "z", "movie", "Godfather", "Hocus Pocus", "Apocalypse Now", "The Muppets");
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

        assertOrderedResultsMatch(query, "x", "movie", "Heat", "Godfather", "Apocalypse Now");
    }

    @Test
    public void testNondistinctQuery() {
        MatchQuery query = qb.match(
                var("x").isa("person"),
                var("y").has("title", "The Muppets"),
                var().rel("x").rel("y")
        ).select("x");
        List<Map<String, Concept>> nondistinctResults = Lists.newArrayList(query);

        QueryUtil.assertResultsMatch(query, "x", "person", graph.getResourceType("name"), "Kermit The Frog", "Miss Piggy");
        assertEquals(4, nondistinctResults.size());
    }

    @Test
    public void testDistinctQuery() {
        MatchQuery query = qb.match(
                var("x").isa("person"),
                var("y").has("title", "The Muppets"),
                var().rel("x").rel("y")
        ).distinct().select("x");
        List<Map<String, Concept>> distinctResults = Lists.newArrayList(query);

        QueryUtil.assertResultsMatch(query, "x", "person", graph.getResourceType("name"), "Kermit The Frog", "Miss Piggy");
        assertEquals(2, distinctResults.size());
    }

    private void assertOrderedResultsMatch(MatchQuery query, String var, String expectedType, String... expectedTitles) {
        Queue<String> expectedQueue = new LinkedList<>(Arrays.asList(expectedTitles));
        ResourceType title = graph.getResourceType("title");

        query.forEach(results -> {
            Concept result = results.get(var);
            assertNotNull(result);

            String expectedTitle = expectedQueue.poll();
            if (expectedTitle != null) {
                //The most lovely lookup ever
                String foundTitle = result.asEntity().resources(title).iterator().next().asResource().getValue().toString();
                assertEquals(expectedTitle, foundTitle);
            }
            if (expectedType != null) assertEquals(expectedType, result.asInstance().type().getName());
        });

        assertTrue("expected titles not found: " + expectedQueue, expectedQueue.isEmpty());
    }

    private void assertResultsOrderedByValue(MatchQuery query, String var, boolean asc) {
        Stream values = query.stream().map(result -> result.get(var).asResource().getValue());
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
