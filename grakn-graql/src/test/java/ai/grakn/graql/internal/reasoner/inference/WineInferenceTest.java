/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.graql.internal.reasoner.inference;

import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.rule.SampleKBContext;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.util.GraqlTestUtil.assertQueriesEqual;

public class WineInferenceTest {

    @ClassRule
    public static final SampleKBContext wineGraph = SampleKBContext.load("wines-test.gql", "wines-rules.gql");

    @Test
    public void testRecommendation() {
        String queryString = "match $x isa person;$y isa wine;($x, $y) isa wine-recommendation;$y has name $wineName; get;";
        QueryBuilder qb = wineGraph.tx().graql().infer(false);
        QueryBuilder iqb = wineGraph.tx().graql().infer(true);

        String explicitQuery = "match $x isa person, has name $nameP;$y isa wine, has name $wineName;" +
                            "{$nameP == 'Bob';$wineName == 'White Champagne';} or" +
                        "{$nameP == 'Alice';$wineName == 'Cabernet Sauvignion';} or" +
                        "{$nameP == 'Charlie';$wineName == 'Pinot Grigio Rose';} or" +
                        "{$nameP == 'Denis';$wineName == 'Busuioaca Romaneasca';} or" +
                        "{$nameP == 'Eva';$wineName == 'Tamaioasa Romaneasca';} or" +
                        "{$nameP == 'Frank';$wineName == 'Riojo Blanco CVNE 2003';}; get $x, $y, $wineName;";

        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }
}
