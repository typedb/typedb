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

import ai.grakn.concept.Concept;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.reasoner.Reasoner;
import ai.grakn.graql.VarName;
import ai.grakn.graphs.GeoGraph;
import ai.grakn.test.GraphContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class GeoInferenceTest {

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(usingTinker());
    }

    @ClassRule
    public static final GraphContext geoGraph = GraphContext.preLoad(GeoGraph.get());

    @Test
    public void testQuery() {
        QueryBuilder qb = geoGraph.graph().graql().infer(false);
        String queryString = "match $x isa city;$x has name $name;"+
                        "(geo-entity: $x, entity-location: $y) isa is-located-in;"+
                        "$y isa country;$y has name 'Poland'; select $x, $name;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$x isa city;$x has name $name;{$name value 'Warsaw';} or {$name value 'Wroclaw';};select $x, $name;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.parse(explicitQuery));
        assertQueriesEqual(Reasoner.resolve(query, true), qb.parse(explicitQuery));
    }

    @Test
    public void testQueryPrime() {
        QueryBuilder qb = geoGraph.graph().graql().infer(false);
        String queryString = "match $x isa city;$x has name $name;"+
                "($x, $y) isa is-located-in;"+
                "$y isa country;$y has name 'Poland'; select $x, $name;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$x isa city;$x has name $name;{$name value 'Warsaw';} or {$name value 'Wroclaw';};select $x, $name;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.parse(explicitQuery));
        assertQueriesEqual(Reasoner.resolve(query, true), qb.parse(explicitQuery));
    }

    @Test
    public void testQuery2() {
        QueryBuilder qb = geoGraph.graph().graql().infer(false);
        String queryString = "match $x isa university;$x has name $name;"+
                "(geo-entity: $x, entity-location: $y) isa is-located-in;"+
                "$y isa country;$y has name 'Poland'; select $x, $name;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match " +
                "$x isa university;$x has name $name;" +
                "{$x has name 'University-of-Warsaw';} or {$x has name'Warsaw-Polytechnics';};";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.parse(explicitQuery));
        assertQueriesEqual(Reasoner.resolve(query, true), qb.parse(explicitQuery));
    }

    @Test
    public void testQuery2Prime() {
        QueryBuilder qb = geoGraph.graph().graql().infer(false);
        String queryString = "match $x isa university;$x has name $name;"+
                "($x, $y) isa is-located-in;"+
                "$y isa country;$y has name 'Poland'; select $x, $name;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match " +
                "$x isa university;$x has name $name;" +
                "{$x has name 'University-of-Warsaw';} or {$x has name'Warsaw-Polytechnics';};";
        assertQueriesEqual(Reasoner.resolve(query, false), qb.parse(explicitQuery));
        assertQueriesEqual(Reasoner.resolve(query, true), qb.parse(explicitQuery));
    }

    private void assertQueriesEqual(Stream<Map<VarName, Concept>> s1, MatchQuery s2) {
        assertEquals(s1.collect(Collectors.toSet()), s2.admin().streamWithVarNames().collect(Collectors.toSet()));
    }
}
