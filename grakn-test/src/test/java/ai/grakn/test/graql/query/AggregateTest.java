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

import ai.grakn.concept.Instance;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.test.AbstractMovieGraphTest;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.AbstractMovieGraphTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static ai.grakn.graql.Graql.average;
import static ai.grakn.graql.Graql.count;
import static ai.grakn.graql.Graql.group;
import static ai.grakn.graql.Graql.max;
import static ai.grakn.graql.Graql.median;
import static ai.grakn.graql.Graql.min;
import static ai.grakn.graql.Graql.select;
import static ai.grakn.graql.Graql.sum;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.test.graql.query.QueryUtil.movies;
import static org.junit.Assert.assertEquals;

public class AggregateTest extends AbstractMovieGraphTest {

    private QueryBuilder qb;

    @Before
    public void setUp() {
        qb = graph.graql();
    }

    @Test
    public void testCount() {
        AggregateQuery<Long> countQuery = qb.match(var("x").isa("movie")).aggregate(count());

        long count = countQuery.execute();

        Assert.assertEquals(QueryUtil.movies.length, count);
    }

    @Test
    public void testGroup() {
        AggregateQuery<Map<Concept, List<Map<String, Concept>>>> groupQuery =
                qb.match(var("x").isa("movie"), var("y").isa("person"), var().rel("x").rel("y")).aggregate(group("x"));

        Map<Concept, List<Map<String, Concept>>> groups = groupQuery.execute();

        Assert.assertEquals(QueryUtil.movies.length, groups.size());

        groups.forEach((movie, results) -> {
            results.forEach(result -> {
                assertEquals(movie, result.get("x"));
                assertEquals(graph.getEntityType("person"), result.get("y").type());
            });
        });
    }

    @Test
    public void testGroupCount() {
        AggregateQuery<Map<Concept, Long>> groupCountQuery =
                qb.match(var("x").isa("movie"), var().rel("x")).aggregate(group("x", count()));

        Map<Concept, Long> groupCount = groupCountQuery.execute();

        Instance godfather = graph.getResource("Godfather", graph.getResourceType("title")).owner();

        assertEquals(new Long(9), groupCount.get(godfather));
    }

    @Test
    public void testCountAndGroup() {
        AggregateQuery<Map<String, Object>> query = qb.match(var("x").isa("movie"), var().rel("x").rel("y"))
                        .aggregate(select(count().as("c"), group("x").as("g")));

        Map<String, Object> results = query.execute();

        long count = (long) results.get("c");

        //noinspection unchecked
        Map<Concept, List<Map<String, Concept>>> groups = (Map<Concept, List<Map<String, Concept>>>) results.get("g");

        Assert.assertEquals(QueryUtil.movies.length, groups.size());

        long groupedResults = groups.values().stream().mapToInt(List::size).sum();

        assertEquals(groupedResults, count);
    }

    @Test
    public void testSumLong() {
        AggregateQuery<Number> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-count"))
                .aggregate(sum("y"));

        assertEquals(1940L, query.execute());
    }

    @Test
    public void testSumDouble() {
        AggregateQuery<Number> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-average"))
                .aggregate(sum("y"));

        assertEquals(27.7d, query.execute().doubleValue(), 0.01d);
    }

    @Test
    public void testMaxLong() {
        AggregateQuery<Optional<?>> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-count"))
                .aggregate(max("y"));

        assertEquals(Optional.of(1000L), query.execute());
    }

    @Test
    public void testMaxDouble() {
        AggregateQuery<Optional<?>> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-average"))
                .aggregate(max("y"));

        assertEquals(Optional.of(8.6d), query.execute());
    }

    @Test
    public void testMaxString() {
        AggregateQuery<Optional<?>> query = qb.match(var("x").isa("title")).aggregate(max("x"));
        assertEquals(Optional.of("The Muppets"), query.execute());
    }

    @Test
    public void testMinLong() {
        AggregateQuery<Optional<?>> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-count"))
                .aggregate(min("y"));

        assertEquals(Optional.of(5L), query.execute());
    }

    @Test
    public void testAverageDouble() {
        AggregateQuery<Optional<Double>> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-average"))
                .aggregate(average("y"));

        //noinspection OptionalGetWithoutIsPresent
        assertEquals((8.6d + 7.6d + 8.4d + 3.1d) / 4d, query.execute().get(), 0.01d);
    }

    @Test
    public void testMedianLong() {
        AggregateQuery<Optional<Number>> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-count"))
                .aggregate(median("y"));

        assertEquals(Optional.of(400L), query.execute());
    }

    @Test
    public void testMedianDouble() {
        AggregateQuery<Optional<Number>> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-average"))
                .aggregate(median("y"));

        //noinspection OptionalGetWithoutIsPresent
        assertEquals(8.0d, query.execute().get().doubleValue(), 0.01d);
    }
}
