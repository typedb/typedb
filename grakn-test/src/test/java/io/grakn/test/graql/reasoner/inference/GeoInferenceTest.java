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
 */

package ai.grakn.test.graql.reasoner.inference;

import com.google.common.collect.Sets;
import ai.grakn.GraknGraph;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Reasoner;
import ai.grakn.test.graql.reasoner.graphs.GeoGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.grakn.graql.internal.reasoner.Utility.printMatchQueryResults;
import static org.junit.Assert.assertEquals;

public class GeoInferenceTest {

    private static GraknGraph graph;
    private static Reasoner reasoner;
    private static QueryBuilder qb;

    @BeforeClass
    public static void setUpClass() {
        graph = GeoGraph.getGraph();
        reasoner = new Reasoner(graph);
        qb = Graql.withGraph(graph);
    }

    @Test
    public void testQuery() {
        String queryString = "match " +
                        "$x isa city;(geo-entity: $x, entity-location: $y) isa is-located-in;\n"+
                        "$y isa country;$y id 'Poland'; select $x;";
        MatchQuery query = qb.parse(queryString);
        printMatchQueryResults(query.distinct());

        String explicitQuery = "match " +
                "$x isa city;{$x id 'Warsaw';} or {$x id 'Wroclaw';};" +
                "$y isa country;$y id 'Poland'; select $x;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testQueryPrime() {
        String queryString = "match " +
                "$x isa city;($x, $y) isa is-located-in;\n"+
                "$y isa country;$y id 'Poland'; select $x;";
        MatchQuery query = qb.parse(queryString);
        printMatchQueryResults(query.distinct());

        String explicitQuery = "match " +
                "$x isa city;{$x id 'Warsaw';} or {$x id 'Wroclaw';};" +
                "$y isa country;$y id 'Poland'; select $x;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testQuery2() {
        String queryString = "match " +
                "$x isa university;(geo-entity: $x, entity-location: $y) isa is-located-in;"+
                "$y isa country;$y id 'Poland'; select $x;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match " +
                "$x isa university;{$x id 'University-of-Warsaw';} or {$x id 'Warsaw-Polytechnics';};" +
                "$y isa country;$y id 'Poland'; select $x;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testQuery2Prime() {
        String queryString = "match " +
                "$x isa university;($x, $y) isa is-located-in;"+
                "$y isa country;$y id 'Poland'; select $x;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match " +
                "$x isa university;{$x id 'University-of-Warsaw';} or {$x id 'Warsaw-Polytechnics';};" +
                "$y isa country;$y id 'Poland'; select $x;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
