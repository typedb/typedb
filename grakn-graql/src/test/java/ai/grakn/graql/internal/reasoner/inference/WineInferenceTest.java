/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.graql.internal.reasoner.inference;

import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.GraknTestUtil;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static ai.grakn.util.GraqlTestUtil.assertQueriesEqual;
import static org.junit.Assume.assumeTrue;

public class WineInferenceTest {

    @Rule
    public final SampleKBContext wineGraph = SampleKBContext.load("wines-test.gql", "wines-rules.gql");

    @BeforeClass
    public static void setUpClass() {
        assumeTrue(GraknTestUtil.usingTinker());
    }

    @Test
    public void testRecommendation() {
        String queryString = "match $x isa person;$y isa wine;($x, $y) isa wine-recommendation;$y has name $nameW; get;";
        QueryBuilder qb = wineGraph.tx().graql().infer(false);
        QueryBuilder iqb = wineGraph.tx().graql().infer(true);

        String explicitQuery = "match $x isa person, has name $nameP;$y isa wine, has name $nameW;" +
                            "{$nameP val 'Bob';$nameW val 'White Champagne';} or" +
                        "{$nameP val 'Alice';$nameW val 'Cabernet Sauvignion';} or" +
                        "{$nameP val 'Charlie';$nameW val 'Pinot Grigio Rose';} or" +
                        "{$nameP val 'Denis';$nameW val 'Busuioaca Romaneasca';} or" +
                        "{$nameP val 'Eva';$nameW val 'Tamaioasa Romaneasca';} or" +
                        "{$nameP val 'Frank';$nameW val 'Riojo Blanco CVNE 2003';}; get $x, $y, $nameW;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }
}
