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
import ai.grakn.concept.Concept;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Reasoner;
import ai.grakn.test.AbstractEngineTest;
import ai.grakn.test.graql.reasoner.graphs.GeoGraph;
import ai.grakn.graql.QueryBuilder;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.BeforeClass;
import org.junit.Test;

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
        QueryBuilder qb = graph.graql().infer(false);
        String queryString = "match $x isa city;$x has name $name;"+
                        "(geo-entity: $x, entity-location: $y) isa is-located-in;"+
                        "$y isa country;$y has name 'Poland'; select $x, $name;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$x isa city;$x has name $name;{$name value 'Warsaw';} or {$name value 'Wroclaw';};select $x, $name;";

        assertQueriesEqual(reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Test
    public void testQueryPrime() {
        GraknGraph graph = GeoGraph.getGraph();
        QueryBuilder qb = graph.graql().infer(false);
        QueryBuilder iqb = graph.graql().infer(true);
        String queryString = "match $z1 isa city;$z1 has name $name;"+
                "($z1, $z2) isa is-located-in;$z2 isa country;$z2 has name 'Poland'; select $z1, $name;";
        String queryString2 = "match $z2 isa city;$z2 has name $name;"+
                "($z1, $z2) isa is-located-in;$z1 isa country;$z1 has name 'Poland'; select $z2, $name;";
        String explicitQuery = "match " +
                "$z1 isa city;$z1 has name $name;{$name value 'Warsaw';} or {$name value 'Wroclaw';};select $z1, $name;";
        String explicitQuery2 = "match " +
                "$z2 isa city;$z2 has name $name;{$name value 'Warsaw';} or {$name value 'Wroclaw';};select $z2, $name;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(false).parse(queryString2), qb.parse(explicitQuery2));
        assertQueriesEqual(iqb.materialise(true).parse(queryString2), qb.parse(explicitQuery2));
    }

    @Test
    public void testQuery2() {
        GraknGraph graph = GeoGraph.getGraph();
        QueryBuilder qb = graph.graql().infer(false);
        QueryBuilder iqb = graph.graql().infer(true);
        String queryString = "match $x isa university;$x has name $name;"+
                "(geo-entity: $x, entity-location: $y) isa is-located-in;"+
                "$y isa country;$y has name 'Poland'; select $x, $name;";
        String explicitQuery = "match " +
                "$x isa university;$x has name $name;" +
                "{$x has name 'University-of-Warsaw';} or {$x has name'Warsaw-Polytechnics';};";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testQuery2Prime() {
        GraknGraph graph = GeoGraph.getGraph();
        QueryBuilder qb = graph.graql().infer(false);
        QueryBuilder iqb = graph.graql().infer(true);
        String queryString = "match $z1 isa university;$z1 has name $name;"+
                "($z1, $z2) isa is-located-in;$z2 isa country;$z2 has name 'Poland'; select $z1, $name;";
        String queryString2 = "match $z2 isa university;$z2 has name $name;"+
                "($z1, $z2) isa is-located-in;$z1 isa country;$z1 has name 'Poland'; select $z2, $name;";
        String explicitQuery = "match " +
                "$z1 isa university;$z1 has name $name;" +
                "{$z1 has name 'University-of-Warsaw';} or {$z1 has name'Warsaw-Polytechnics';};";
        String explicitQuery2 = "match " +
                "$z2 isa university;$z2 has name $name;" +
                "{$z2 has name 'University-of-Warsaw';} or {$z2 has name'Warsaw-Polytechnics';};";
        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(false).parse(queryString2), qb.parse(explicitQuery2));
        assertQueriesEqual(iqb.materialise(true).parse(queryString2), qb.parse(explicitQuery2));
    }

    private void assertQueriesEqual(Stream<Map<String, Concept>> s1, Stream<Map<String, Concept>> s2) {
        assertEquals(s1.collect(Collectors.toSet()), s2.collect(Collectors.toSet()));
    }
}
