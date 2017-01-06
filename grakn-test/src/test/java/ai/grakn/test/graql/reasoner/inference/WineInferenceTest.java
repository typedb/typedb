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
import ai.grakn.graql.Reasoner;
import ai.grakn.graql.VarName;
import ai.grakn.test.AbstractEngineTest;
import ai.grakn.test.graql.reasoner.graphs.TestGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;


public class WineInferenceTest extends AbstractEngineTest{

    private static Reasoner reasoner;
    private static QueryBuilder qb;

    @BeforeClass
    public static void setUpClass() {
        assumeTrue(usingTinker());
        GraknGraph graph = TestGraph.getGraph("name", "wines-test.gql", "wines-rules.gql");
        reasoner = new Reasoner(graph);
        qb = graph.graql().infer(false);
    }

    @Test
    public void testRecommendation() {
        String queryString = "match $x isa person;$y isa wine;($x, $y) isa wine-recommendation;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $x isa person, has name $nameP;$y isa wine, has name $nameW;" +
                            "{$nameP value 'Bob';$nameW value 'White Champagne';} or" +
                        "{$nameP value 'Alice';$nameW value 'Cabernet Sauvignion';} or" +
                        "{$nameP value 'Charlie';$nameW value 'Pinot Grigio Rose';} or" +
                        "{$nameP value 'Denis';$nameW value 'Busuioaca Romaneasca';} or" +
                        "{$nameP value 'Eva';$nameW value 'Tamaioasa Romaneasca';} or" +
                        "{$nameP value 'Frank';$nameW value 'Riojo Blanco CVNE 2003';}; select $x, $y;";

        assertQueriesEqual(reasoner.resolve(query, false), qb.parse(explicitQuery));
        assertQueriesEqual(reasoner.resolve(query, true), qb.parse(explicitQuery));
    }

    private void assertQueriesEqual(Stream<Map<VarName, Concept>> s1, MatchQuery s2) {
        assertEquals(s1.collect(Collectors.toSet()), s2.admin().streamWithVarNames().collect(Collectors.toSet()));
    }
}
