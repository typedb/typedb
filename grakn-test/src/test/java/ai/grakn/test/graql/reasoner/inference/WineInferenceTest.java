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
import ai.grakn.test.AbstractEngineTest;
import com.google.common.collect.Sets;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Reasoner;
import ai.grakn.test.graql.reasoner.graphs.TestGraph;
import org.junit.BeforeClass;
import org.junit.Test;

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
        qb = graph.graql();
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

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
