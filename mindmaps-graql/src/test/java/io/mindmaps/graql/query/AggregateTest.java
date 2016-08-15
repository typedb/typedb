/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package io.mindmaps.graql.query;

import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Instance;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static io.mindmaps.graql.Aggregate.count;
import static io.mindmaps.graql.Aggregate.group;
import static io.mindmaps.graql.QueryBuilder.var;
import static io.mindmaps.graql.query.QueryUtil.movies;
import static org.junit.Assert.assertEquals;

public class AggregateTest {

    private QueryBuilder qb;
    private MindmapsTransaction transaction;

    @Before
    public void setUp() {
        MindmapsGraph mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
        transaction = mindmapsGraph.newTransaction();
        qb = QueryBuilder.build(transaction);
    }

    @Test
    public void testCount() {
        MatchQuery<Long> countQuery = qb.match(var("x").isa("movie")).aggregate(count());

        long count = countQuery.iterator().next();

        assertEquals(movies.length, count);
    }

    @Test
    public void testGroup() {
        MatchQuery<Map<Concept, List<Map<String, Concept>>>> groupQuery =
                qb.match(var("x").isa("movie"), var("y").isa("person"), var().rel("x").rel("y")).aggregate(group("x"));

        assertEquals(new Long(1), groupQuery.aggregate(count()).iterator().next());

        Map<Concept, List<Map<String, Concept>>> groups = groupQuery.iterator().next();

        assertEquals(movies.length, groups.size());

        groups.forEach((movie, results) -> {
            results.forEach(result -> {
                assertEquals(movie, result.get("x"));
                assertEquals(transaction.getEntityType("person"), result.get("y").type());
            });
        });
    }

    @Test
    public void testGroupCount() {
        MatchQuery<Map<Concept, Long>> groupCountQuery =
                qb.match(var("x").isa("movie"), var().rel("x")).aggregate(group("x", count()));

        Map<Concept, Long> groupCount = groupCountQuery.iterator().next();

        Instance godfather = transaction.getInstance("Godfather");

        assertEquals(new Long(9), groupCount.get(godfather));
    }

    @Test
    public void testCountAndGroup() {
        MatchQuery<Map<String, Object>> query =
                qb.match(var("x").isa("movie"), var().rel("x").rel("y")).aggregate(count().as("c"), group("x").as("g"));

        Map<String, Object> results = query.iterator().next();

        long count = (long) results.get("c");
        Map<Concept, List<Map<String, Concept>>> groups = (Map<Concept, List<Map<String, Concept>>>) results.get("g");

        assertEquals(movies.length, groups.size());

        long groupedResults = groups.values().stream().mapToInt(List::size).sum();

        assertEquals(groupedResults, count);
    }
}
