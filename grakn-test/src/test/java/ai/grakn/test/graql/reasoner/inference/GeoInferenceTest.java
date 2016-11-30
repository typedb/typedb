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

package ai.grakn.test.graql.reasoner.inference;

import ai.grakn.GraknGraph;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Reasoner;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.test.AbstractEngineTest;
import ai.grakn.test.graql.reasoner.graphs.GeoGraph;
import com.google.common.collect.Sets;
import ai.grakn.graql.QueryBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import static ai.grakn.graql.internal.reasoner.Utility.printAnswers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class GeoInferenceTest extends AbstractEngineTest {

    @BeforeClass
    public static void onStartup(){
        assumeTrue(usingTinker());
    }

    @Test
    public void testQuery() {
        GraknGraph graph = GeoGraph.getGraph();
        Reasoner reasoner = new Reasoner(graph);
        QueryBuilder qb = graph.graql();
        String queryString = "match $x isa city;$x has name $name;"+
                        "(geo-entity: $x, entity-location: $y) isa is-located-in;"+
                        "$y isa country;$y has name 'Poland'; select $x, $name;";
        MatchQuery query = new Query(queryString, graph);

        String explicitQuery = "match " +
                "$x isa city;$x has name $name;{$name value 'Warsaw';} or {$name value 'Wroclaw';};select $x, $name;";

        QueryAnswers answers = reasoner.resolve(query);
        QueryAnswers explicitAnswers = new QueryAnswers(qb.<MatchQuery>parse(explicitQuery).execute());
        assertEquals(answers, explicitAnswers);
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testQueryPrime() {
        GraknGraph graph = GeoGraph.getGraph();
        Reasoner reasoner = new Reasoner(graph);
        QueryBuilder qb = graph.graql();
        String queryString = "match $x isa city;$x has name $name;"+
                "($x, $y) isa is-located-in;"+
                "$y isa country;$y has name 'Poland'; select $x, $name;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$x isa city;$x has name $name;{$name value 'Warsaw';} or {$name value 'Wroclaw';};select $x, $name;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testQuery2() {
        GraknGraph graph = GeoGraph.getGraph();
        Reasoner reasoner = new Reasoner(graph);
        QueryBuilder qb = graph.graql();
        String queryString = "match $x isa university;$x has name $name;"+
                "(geo-entity: $x, entity-location: $y) isa is-located-in;"+
                "$y isa country;$y has name 'Poland'; select $x, $name;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match " +
                "$x isa university;$x has name $name;" +
                "{$x has name 'University-of-Warsaw';} or {$x has name'Warsaw-Polytechnics';};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testQuery2Prime() {
        GraknGraph graph = GeoGraph.getGraph();
        Reasoner reasoner = new Reasoner(graph);
        QueryBuilder qb = graph.graql();
        String queryString = "match $x isa university;$x has name $name;"+
                "($x, $y) isa is-located-in;"+
                "$y isa country;$y has name 'Poland'; select $x, $name;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match " +
                "$x isa university;$x has name $name;" +
                "{$x has name 'University-of-Warsaw';} or {$x has name'Warsaw-Polytechnics';};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
