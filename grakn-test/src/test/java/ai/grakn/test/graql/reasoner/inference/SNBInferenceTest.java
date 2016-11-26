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
import ai.grakn.test.graql.reasoner.graphs.SNBGraph;
import com.google.common.collect.Sets;
import ai.grakn.graql.QueryBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class SNBInferenceTest extends AbstractEngineTest{

    @BeforeClass
    public static void onStartup(){
        assumeTrue(usingTinker());
    }

    /**
     * Tests transitivity
     */
    @Test
    public void testTransitivity() {
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match " +
                "$x isa university;$y isa country;(located-subject: $x, subject-location: $y) isa resides;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $x isa university, has name 'University of Cambridge';" +
                "$y isa country, has name 'UK';";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testTransitivityPrime() {
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match " +
                "$x isa university;$y isa country;($x, $y) isa resides;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $x isa university, has name 'University of Cambridge';" +
                "$y isa country, has name 'UK';";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    /**
     * Tests transitivity
     */
    @Test
    public void testTransitivity2() {
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $x isa company;$y isa country;" +
                "(located-subject: $x, subject-location: $y) isa resides;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$x isa company, has name 'Grakn';" +
                "$y isa country, has name 'UK';";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testTransitivity2Prime() {
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $x isa company;$y isa country;" +
                "($x, $y) isa resides;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$x isa company, has name 'Grakn';" +
                "$y isa country, has name 'UK';";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    /**
     * Tests relation filtering and rel vars matching
     */
    @Test
    public void testTag() {
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match " +
                "$x isa person;$y isa tag;($x, $y) isa recommendation;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$x isa person, has name $xName;$y isa tag, has name $yName;" +
                "{$xName value 'Charlie';" +
                "{$yName value 'Yngwie Malmsteen';} or {$yName value 'Cacophony';} or" +
                "{$yName value 'Steve Vai';} or {$yName value 'Black Sabbath';};} or " +
                "{$xName value 'Gary';$yName value 'Pink Floyd';};select $x, $y;";

        //assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testTagVarSub() {
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match " +
                "$y isa person;$t isa tag;($y, $t) isa recommendation;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $y isa person, has name $yName;$t isa tag, has name $tName;" +
                "{$yName value 'Charlie';" +
                "{$tName value 'Yngwie Malmsteen';} or {$tName value 'Cacophony';} or" +
                "{$tName value 'Steve Vai';} or {$tName value 'Black Sabbath';};} or " +
                "{$yName value 'Gary';$tName value 'Pink Floyd';};select $y, $t;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    /**
     * Tests relation filtering and rel vars matching
     */
    @Test
    public void testProduct() {
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match " +
                "$x isa person;$y isa product;($x, $y) isa recommendation;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $x isa person, has name $xName;$y isa product, has name $yName;" +
                "{$xName value 'Alice';$yName value 'War of the Worlds';} or" +
                "{$xName value 'Bob';{$yName value 'Ducatti 1299';} or {$yName value 'The Good the Bad the Ugly';};} or" +
                "{$xName value 'Charlie';{$yName value 'Blizzard of Ozz';} or {$yName value 'Stratocaster';};} or " +
                "{$xName value 'Denis';{$yName value 'Colour of Magic';} or {$yName value 'Dorian Gray';};} or"+
                "{$xName value 'Frank';$yName value 'Nocturnes';} or" +
                "{$xName value 'Karl Fischer';{$yName value 'Faust';} or {$yName value 'Nocturnes';};} or " +
                "{$xName value 'Gary';$yName value 'The Wall';};select $x, $y;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testProductVarSub() {
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match " +
                "$y isa person;$yy isa product;($y, $yy) isa recommendation;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $y isa person, has name $ny; $yy isa product, has name $nyy;" +
                "{$ny value 'Alice';$nyy value 'War of the Worlds';} or" +
                "{$ny value 'Bob';{$nyy value 'Ducatti 1299';} or {$nyy value 'The Good the Bad the Ugly';};} or" +
                "{$ny value 'Charlie';{$nyy value 'Blizzard of Ozz';} or {$nyy value 'Stratocaster';};} or " +
                "{$ny value 'Denis';{$nyy value 'Colour of Magic';} or {$nyy value 'Dorian Gray';};} or"+
                "{$ny value 'Frank';$nyy value 'Nocturnes';} or" +
                "{$ny value 'Karl Fischer';{$nyy value 'Faust';} or {$nyy value 'Nocturnes';};} or " +
                "{$ny value 'Gary';$nyy value 'The Wall';};select $y, $yy;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testCombinedProductTag() {
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match " +
                "$x isa person;{$y isa product;} or {$y isa tag;};($x, $y) isa recommendation;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $x isa person;" +
                "{$x has name 'Alice';$y has name 'War of the Worlds';} or" +
                "{$x has name 'Bob';{$y has name 'Ducatti 1299';} or {$y has name 'The Good the Bad the Ugly';};} or" +
                "{$x has name 'Charlie';{$y has name 'Blizzard of Ozz';} or {$y has name 'Stratocaster';};} or " +
                "{$x has name 'Denis';{$y has name 'Colour of Magic';} or {$y has name 'Dorian Gray';};} or"+
                "{$x has name 'Frank';$y has name 'Nocturnes';} or" +
                "{$x has name 'Karl Fischer';{$y has name 'Faust';} or {$y has name 'Nocturnes';};} or " +
                "{$x has name 'Gary';$y has name 'The Wall';} or" +
                "{$x has name 'Charlie';" +
                "{$y has name 'Yngwie Malmsteen';} or {$y has name 'Cacophony';} or {$y has name 'Steve Vai';} or {$y has name 'Black Sabbath';};} or " +
                "{$x has name 'Gary';$y has name 'Pink Floyd';};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testCombinedProductTag2() {
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match " +
                "{$p isa person;$r isa product;($p, $r) isa recommendation;} or" +
                "{$p isa person;$r isa tag;($p, $r) isa recommendation;};";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $p isa person;" +
                "{$p has name 'Alice';$r has name 'War of the Worlds';} or" +
                "{$p has name 'Bob';{$r has name 'Ducatti 1299';} or {$r has name 'The Good the Bad the Ugly';};} or" +
                "{$p has name 'Charlie';{$r has name 'Blizzard of Ozz';} or {$r has name 'Stratocaster';};} or " +
                "{$p has name 'Denis';{$r has name 'Colour of Magic';} or {$r has name 'Dorian Gray';};} or"+
                "{$p has name 'Frank';$r has name 'Nocturnes';} or" +
                "{$p has name 'Karl Fischer';{$r has name 'Faust';} or {$r has name 'Nocturnes';};} or " +
                "{$p has name 'Gary';$r has name 'The Wall';} or" +
                "{$p has name 'Charlie';" +
                "{$r has name 'Yngwie Malmsteen';} or {$r has name 'Cacophony';} or {$r has name 'Steve Vai';} or {$r has name 'Black Sabbath';};} or " +
                "{$p has name 'Gary';$r has name 'Pink Floyd';};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testBook() {
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $x isa person;" +
                "($x, $y) isa recommendation;" +
                "$c isa category;$c has name 'book';" +
                "($y, $c) isa typing; select $x, $y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $x isa person, has name $nx;$y isa product, has name $ny;" +
                "{$nx value 'Alice';$ny value 'War of the Worlds';} or" +
                "{$nx value 'Karl Fischer';$ny value 'Faust';} or " +
                "{$nx value 'Denis';{$ny value 'Colour of Magic';} or {$ny value 'Dorian Gray';};};select $x, $y;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testBand() {
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $x isa person;" +
                "($x, $y) isa recommendation;" +
                "$c isa category;$c has name 'Band';" +
                "($y, $c) isa grouping; select $x, $y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "{$x has name 'Charlie';{$y has name 'Cacophony';} or {$y has name 'Black Sabbath';};} or " +
                "{$x has name 'Gary';$y has name 'Pink Floyd';}; select $x, $y;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    /**
     * Tests global variable consistency (Bug #7344)
     */
    @Test
    public void testVarConsistency(){
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $x isa person;$y isa product;" +
                    "($x, $y) isa recommendation;" +
                    "$z isa category;$z has name 'motorbike';" +
                    "($y, $z) isa typing; select $x, $y;";

        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $x isa person;$y isa product;" +
                "{$x has name 'Bob';$y has name 'Ducatti 1299';};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    /**
     * tests whether rules are filtered correctly (rules recommending products other than Chopin should not be attached)
     */
    @Test
    public void testVarConsistency2(){
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        //select people that have Chopin as a recommendation
        String queryString = "match $x isa person; $y isa tag; ($x, $y) isa tagging;" +
                        "$z isa product;$z has name 'Nocturnes'; ($x, $z) isa recommendation; select $x, $y;";

        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "{$x has name 'Frank';$y has name 'Ludwig van Beethoven';} or" +
                "{$x has name 'Karl Fischer';" +
                "{$y has name 'Ludwig van Beethoven';} or {$y has name 'Johann Wolfgang von Goethe';} or" +
                "{$y has name 'Wolfgang Amadeus Mozart';};};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testVarConsistency3(){
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $x isa person;$pr isa product, has name 'Nocturnes';($x, $pr) isa recommendation; select $x;";
        Query query = new Query(queryString, graph);

        String explicitQuery = "match {$x has name 'Frank';} or {$x has name 'Karl Fischer';};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    /**
     * Tests transitivity and Bug #7416
     */
    @Test
    public void testQueryConsistency() {
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $x isa person; $y isa place; ($x, $y) isa resides;" +
                        "$z isa person;$z has name 'Miguel Gonzalez'; ($x, $z) isa knows; select $x, $y;";
        MatchQuery query = qb.parse(queryString);
        QueryAnswers answers = new QueryAnswers(reasoner.resolve(query));

        String queryString2 = "match $x isa person; $y isa person;$y has name 'Miguel Gonzalez';" +
                        "$z isa place; ($x, $y) isa knows; ($x, $z) isa resides; select $x, $z;";
        MatchQuery query2 = qb.parse(queryString2);
        Map<String, String> unifiers = new HashMap<>();
        unifiers.put("z", "y");

        QueryAnswers answers2 = (new QueryAnswers(reasoner.resolve(query2))).unify(unifiers);
        assertEquals(answers, answers2);
    }

    /**
     * Tests Bug #7416
     * the $t variable in the query matches with $t from rules so if the rule var is not changed an extra condition is created
     * which renders the query unsatisfiable
     */
    @Test
    public void testOrdering() {
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        //select recommendationS of Karl Fischer and their types
        String queryString = "match $p isa product;$x isa person;$x has name 'Karl Fischer';" +
                        "($x, $p) isa recommendation; ($p, $t) isa typing; select $p, $t;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $p isa product;" +
                "$x isa person;$x has name 'Karl Fischer';{($x, $p) isa recommendation;} or" +
                "{$x isa person;$tt isa tag;$tt has name 'Johann Wolfgang von Goethe';" +
                "($x, $tt) isa tagging;$p isa product;$p has name 'Faust';} or" +
                "{$x isa person; $p isa product;$p has name 'Nocturnes'; $tt isa tag; ($tt, $x), isa tagging;};" +
                "($p, $t) isa typing; select $p, $t;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testOrdering2() {
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        //select recommendationS of Karl Fischer and their types
        String queryString2 = "match $p isa product;$x isa person;$x has name 'Karl Fischer';" +
                "($p, $c) isa typing; ($x, $p) isa recommendation; select $p, $c;";
        MatchQuery query2 = qb.parse(queryString2);

        String explicitQuery2 = "match $p isa product;\n" +
                "$x isa person;$x has name 'Karl Fischer';{($x, $p) isa recommendation;} or" +
                "{$x isa person;$t isa tag, has name 'Johann Wolfgang von Goethe';" +
                "($x, $t) isa tagging;$p isa product;$p has name 'Faust';} or" +
                "{$x isa person; $p isa product;$p has name 'Nocturnes'; $t isa tag; ($t, $x), isa tagging;};" +
                "($p, $c) isa typing; select $p, $c;";

        assertEquals(reasoner.resolve(query2), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery2)));
        assertQueriesEqual(reasoner.resolveToQuery(query2), qb.parse(explicitQuery2));
    }

    /**
     * Tests Bug #7422
     */
    @Test
    public void testInverseVars() {
        GraknGraph graph = SNBGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        //select recommendation of Karl Fischer and their types
        String queryString = "match $p isa product;" +
                "$x isa person;$x has name 'Karl Fischer'; ($p, $x) isa recommendation; ($p, $t) isa typing; select $p, $t;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $p isa product;" +
                "$x isa person;$x has name 'Karl Fischer';{($x, $p) isa recommendation;} or" +
                "{$x isa person; $p isa product;$p has name 'Nocturnes'; $tt isa tag; ($tt, $x), isa tagging;} or" +
                "{$x isa person;$tt isa tag;$tt has name 'Johann Wolfgang von Goethe';($x, $tt) isa tagging;" +
                "$p isa product;$p has name 'Faust';};" +
                "($p, $t) isa typing; select $p, $t;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
