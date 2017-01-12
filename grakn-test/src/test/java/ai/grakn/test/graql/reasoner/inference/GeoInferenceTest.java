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
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.reasoner.Reasoner;
import ai.grakn.graql.VarName;
import ai.grakn.test.AbstractGraknTest;
import ai.grakn.test.graql.reasoner.graphs.GeoGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class GeoInferenceTest extends AbstractGraknTest {
    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(usingTinker());
    }

    @Test
    public void testQuery() {
        GraknGraph graph = GeoGraph.getGraph();
        QueryBuilder qb = graph.graql().infer(false);
        String queryString = "match $x isa city;$x has name $name;"+
                        "(geo-entity: $x, entity-location: $y) isa is-located-in;"+
                        "$y isa country;$y has name 'Poland'; select $x, $name;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$x isa city;$x has name $name;{$name value 'Warsaw';} or {$name value 'Wroclaw';};select $x, $name;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery));
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery));
    }

    @Test
    public void testQueryPrime() {
        GraknGraph graph = GeoGraph.getGraph();
                QueryBuilder qb = graph.graql().infer(false);
        String queryString = "match $x isa city;$x has name $name;"+
                "($x, $y) isa is-located-in;"+
                "$y isa country;$y has name 'Poland'; select $x, $name;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$x isa city;$x has name $name;{$name value 'Warsaw';} or {$name value 'Wroclaw';};select $x, $name;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery));
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery));
    }

    @Test
    public void testQuery2() {
        GraknGraph graph = GeoGraph.getGraph();
                QueryBuilder qb = graph.graql().infer(false);
        String queryString = "match $x isa university;$x has name $name;"+
                "(geo-entity: $x, entity-location: $y) isa is-located-in;"+
                "$y isa country;$y has name 'Poland'; select $x, $name;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match " +
                "$x isa university;$x has name $name;" +
                "{$x has name 'University-of-Warsaw';} or {$x has name'Warsaw-Polytechnics';};";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery));
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery));
    }

    @Test
    public void testQuery2Prime() {
        GraknGraph graph = GeoGraph.getGraph();
                QueryBuilder qb = graph.graql().infer(false);
        String queryString = "match $x isa university;$x has name $name;"+
                "($x, $y) isa is-located-in;"+
                "$y isa country;$y has name 'Poland'; select $x, $name;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match " +
                "$x isa university;$x has name $name;" +
                "{$x has name 'University-of-Warsaw';} or {$x has name'Warsaw-Polytechnics';};";
        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery));
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery));
    }

    private void assertQueriesEqual(Stream<Map<VarName, Concept>> s1, MatchQuery s2) {
        assertEquals(s1.collect(Collectors.toSet()), s2.admin().streamWithVarNames().collect(Collectors.toSet()));
    }
}
