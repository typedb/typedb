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

package io.mindmaps.graql.reasoner.inference;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Rule;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.graql.reasoner.graphs.SNBGraph;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static io.mindmaps.graql.internal.reasoner.Utility.isRuleRecursive;
import static io.mindmaps.graql.internal.reasoner.Utility.printMatchQueryResults;
import static org.junit.Assert.assertEquals;

public class SNBInferenceTest {

    private static MindmapsGraph graph;
    private static Reasoner reasoner;
    private static QueryBuilder qb;

    @BeforeClass
    public static void setUpClass() {
        graph = SNBGraph.getGraph();
        qb = Graql.withGraph(graph);
        reasoner = new Reasoner(graph);
    }

    @Test
    public void testRecursive() {
        Rule R1 = graph.getRule("R1");
        Rule R2 = graph.getRule("R2");
        assertEquals(true, isRuleRecursive(R1) && isRuleRecursive(R2));
    }

    /**
     * Tests transitivity
     */
    @Test
    public void testTransitivity() {
        String queryString = "match " +
                "$x isa university;$y isa country;($x, $y) isa resides";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "$x isa university;$x id 'University of Cambridge';$y isa country;$y id 'UK'";

        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
    }

    /**
     * Tests transitivity for non-Horn clause query
     */
    @Test
    public void testTransitivity2() {
        String queryString = "match " +
                "{$x isa university} or {$x isa company};\n" +
                "$y isa country;\n" +
                "($x, $y) isa resides";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "{$x isa university;$x id 'University of Cambridge'} or" +
                "{$x isa company;$x id 'Mindmaps'};" +
                "$y isa country;$y id 'UK'";

        assertQueriesEqual(reasoner.expand(query), qb.parseMatch(explicitQuery).getMatchQuery());
    }

    @Test
    public void testTransitivity3() {
        String queryString = "match " +
                "{$y isa university} or {$y isa company};\n" +
                "$x isa country;\n" +
                "(subject-location $x, located-subject $y) isa resides";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "{$y isa university;$y id 'University of Cambridge'} or" +
                "{$y isa company;$y id 'Mindmaps'};" +
                "$x isa country;$x id 'UK'";

        assertQueriesEqual(reasoner.expand(query), qb.parseMatch(explicitQuery).getMatchQuery());
    }

    /**
     * Tests transitivity and Bug #7343
     */
    @Test
    public void testTransitivity4() {
        String queryString = " match" +
                "{$x isa university} or {$x isa company};\n" +
                "$y isa continent;\n" +
                "($x, $y) isa resides";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String explicitQuery = "match " +
                            "{$x isa university} or {$x isa company};\n" +
                            "$y isa continent;\n" +
                            "{($x, $y) isa resides} or\n" +
                            "{($x, $yy) isa resides; {(container-location $y, member-location $yy) isa sublocate} or\n" +
                            "{(container-location $y, member-location $yyyy) isa sublocate; (container-location $yyyy, member-location $yy) isa sublocate}}" +
                            "select $x, $y";

        assertQueriesEqual(reasoner.expand(query), qb.parseMatch(explicitQuery).getMatchQuery());
    }

    /**
     * Tests relation filtering and rel vars matching
     */
    @Test
    public void testTag() {
        String queryString = "match " +
                "$x isa person;$y isa tag;($x, $y) isa recommendation";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "$x isa person;$y isa tag;" +
                "{$x id 'Charlie';{$y id 'Yngwie Malmsteen'} or {$y id 'Cacophony'} or {$y id 'Steve Vai'} or {$y id 'Black Sabbath'}} or " +
                "{$x id 'Gary';$y id 'Pink Floyd'}";

        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
    }

    @Test
    public void testTagVarSub() {
        String queryString = "match " +
                "$y isa person;$t isa tag;($y, $t) isa recommendation";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "$y isa person;$t isa tag;" +
                "{$y id 'Charlie';" +
                "{$t id 'Yngwie Malmsteen'} or {$t id 'Cacophony'} or {$t id 'Steve Vai'} or {$t id 'Black Sabbath'}} or " +
                "{$y id 'Gary';$t id 'Pink Floyd'}";

        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
    }

    /**
     * Tests relation filtering and rel vars matching
     */
    @Test
    public void testProduct() {
        String queryString = "match " +
                "$x isa person;$y isa product;($x, $y) isa recommendation";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "$x isa person;$y isa product;" +
                "{$x id 'Alice';$y id 'War of the Worlds'} or" +
                "{$x id 'Bob';{$y id 'Ducatti 1299'} or {$y id 'The Good the Bad the Ugly'}} or" +
                "{$x id 'Charlie';{$y id 'Blizzard of Ozz'} or {$y id 'Stratocaster'}} or " +
                "{$x id 'Denis';{$y id 'Colour of Magic'} or {$y id 'Dorian Gray'}} or"+
                "{$x id 'Frank';$y id 'Nocturnes'} or" +
                "{$x id 'Karl Fischer';{$y id 'Faust'} or {$y id 'Nocturnes'}} or " +
                "{$x id 'Gary';$y id 'The Wall'}";

        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
    }

    @Test
    public void testProductVarSub() {
        String queryString = "match " +
                "$y isa person;$yy isa product;($y, $yy) isa recommendation";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "$y isa person;$yy isa product;" +
                "{$y id 'Alice';$yy id 'War of the Worlds'} or" +
                "{$y id 'Bob';{$yy id 'Ducatti 1299'} or {$yy id 'The Good the Bad the Ugly'}} or" +
                "{$y id 'Charlie';{$yy id 'Blizzard of Ozz'} or {$yy id 'Stratocaster'}} or " +
                "{$y id 'Denis';{$yy id 'Colour of Magic'} or {$yy id 'Dorian Gray'}} or"+
                "{$y id 'Frank';$yy id 'Nocturnes'} or" +
                "{$y id 'Karl Fischer';{$yy id 'Faust'} or {$yy id 'Nocturnes'}} or " +
                "{$y id 'Gary';$yy id 'The Wall'}";

        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
    }

    @Test
    @Ignore
    public void testCombinedProductTag() {
        String queryString = "match " +
                "{$x isa person;{$y isa product} or {$y isa tag};($x, $y) isa recommendation}";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "{$x isa person;$y isa product;" +
                "{$x id 'Alice';$y id 'war-of-the-worlds'} or" +
                "{$x id 'Bob';{$y id 'Ducatti-1299'} or {$y id 'The-good-the-bad-the-ugly'}} or" +
                "{$x id 'Charlie';{$y id 'blizzard-of-ozz'} or {$y id 'stratocaster'}} or " +
                "{$x id 'Denis';{$y id 'colour-of-magic'} or {$y id 'dorian-gray'}} or"+
                "{$x id 'Frank';$y id 'nocturnes'} or" +
                "{$x id 'Karl Fischer';{$y id 'faust'} or {$y id 'nocturnes'}} or " +
                "{$x id 'Gary';$y id 'the-wall'}} or" +
                "{$x isa person;$y isa tag;" +
                "{$x id 'Charlie';{$y id 'yngwie-malmsteen'} or {$y id 'cacophony'} or {$y id 'steve-vai'} or {$y id 'black-sabbath'}} or " +
                "{$x id 'Gary';$y id 'pink-floyd'}}";

        assertQueriesEqual(reasoner.expand(query), qb.parseMatch(explicitQuery).getMatchQuery());
    }

    @Test
    @Ignore
    public void testCombinedProductTag2() {
        String queryString = "match " +
                "{$x isa person;$y isa product;($x, $y) isa recommendation} or" +
                "{$x isa person;$y isa tag;($x, $y) isa recommendation}";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "{$x isa person;$y isa product;" +
                "{$x id 'Alice';$y id 'war-of-the-worlds'} or" +
                "{$x id 'Bob';{$y id 'Ducatti-1299'} or {$y id 'The-good-the-bad-the-ugly'}} or" +
                "{$x id 'Charlie';{$y id 'blizzard-of-ozz'} or {$y id 'stratocaster'}} or " +
                "{$x id 'Denis';{$y id 'colour-of-magic'} or {$y id 'dorian-gray'}} or"+
                "{$x id 'Frank';$y id 'nocturnes'} or" +
                "{$x id 'Karl Fischer';{$y id 'faust'} or {$y id 'nocturnes'}} or " +
                "{$x id 'Gary';$y id 'the-wall'}} or" +
                "{$x isa person;$y isa tag;" +
                "{$x id 'Charlie';{$y id 'yngwie-malmsteen'} or {$y id 'cacophony'} or {$y id 'steve-vai'} or {$y id 'black-sabbath'}} or " +
                "{$x id 'Gary';$y id 'pink-floyd'}}";

        assertQueriesEqual(reasoner.expand(query), qb.parseMatch(explicitQuery).getMatchQuery());
    }

    @Test
    public void testBook() {
        String queryString = "match $x isa person;\n" +
                "($x, $y) isa recommendation;\n" +
                "$c isa category;$c id 'book';\n" +
                "($y, $c) isa typing; select $x, $y";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "$x isa person;$y isa product;" +
                "{$x id 'Alice';$y id 'War of the Worlds'} or" +
                "{$x id 'Karl Fischer';$y id 'Faust'} or " +
                "{$x id 'Denis';{$y id 'Colour of Magic'} or {$y id 'Dorian Gray'}}";

        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
    }

    @Test
    public void testBand() {
        String queryString = "match $x isa person;\n" +
                "($x, $y) isa recommendation;\n" +
                "$c isa category;$c id 'Band';\n" +
                "($y, $c) isa grouping; select $x, $y";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String explicitQuery = "match $x isa person;$y isa tag;" +
                "{$x id 'Charlie';{$y id 'Cacophony'} or {$y id 'Black Sabbath'}} or " +
                "{$x id 'Gary';$y id 'Pink Floyd'}";

        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
    }

    /**
     * Tests global variable consistency (Bug #7344)
     */
    @Test
    public void testVarConsistency(){
        String queryString = "match $x isa person;$y isa product;\n" +
                    "($x, $y) isa recommendation;\n" +
                    "$z isa category;$z id 'motorbike';\n" +
                    "($y, $z) isa typing; select $x, $y";

        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String explicitQuery = "match $x isa person;$y isa product;" +
                "{$x id 'Bob';$y id 'Ducatti 1299'}";

        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
    }

    /**
     * tests whether rules are filtered correctly (rules recommending products other than Chopin should not be attached)
     */
    @Test
    public void testVarConsistency2(){
        //select people that have Chopin as a recommendation
        String queryString = "match $x isa person; $y isa tag; ($x, $y) isa tagging;\n" +
                        "$z isa product;$z id 'Nocturnes'; ($x, $z) isa recommendation; select $x, $y";

        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "{$x id 'Frank';$y id 'Ludwig van Beethoven'} or" +
                "{$x id 'Karl Fischer';" +
                "{$y id 'Ludwig van Beethoven'} or {$y id 'Johann Wolfgang von Goethe'} or {$y id 'Wolfgang Amadeus Mozart'}}";

        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
    }

    @Test
    public void testVarConsistency3(){

        String queryString = "match $x isa person;$pr isa product, id 'Nocturnes';($x, $pr) isa recommendation; select $x";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String explicitQuery = "match {$x id 'Frank'} or {$x id 'Karl Fischer'}";

        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
    }

    /**
     * Tests transitivity and Bug #7416
     */
    @Test
    public void testQueryConsistency() {

        String queryString = "match $x isa person; $y isa place; ($x, $y) isa resides;" +
                        "$z isa person;$z id 'Miguel Gonzalez'; ($x, $z) isa knows; select $x, $y";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expand(query);
        printMatchQueryResults(expandedQuery);

        System.out.println();

        String queryString2 = "match $x isa person; $y isa person;$y id 'Miguel Gonzalez';" +
                        "$z isa place; ($x, $y) isa knows; ($x, $z) isa resides; select $x, $z";
        MatchQuery query2 = qb.parseMatch(queryString2).getMatchQuery();
        MatchQuery expandedQuery2 = reasoner.expand(query2);

        printMatchQueryResults(expandedQuery2);
    }

    /**
     * Tests Bug #7416
     * the $t variable in the query matches with $t from rules so if the rule var is not changed an extra condition is created
     * which renders the query unsatisfiable
     */
    @Test
    public void testOrdering() {
        //select recommendationS of Karl Fischer and their types
        String queryString = "match $p isa product;$x isa person;$x id 'Karl Fischer';" +
                        "($x, $p) isa recommendation; ($p, $t) isa typing; select $p, $t";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String queryString2 = "match $p isa product;$x isa person;$x id 'Karl Fischer';" +
                        "($p, $c) isa typing; ($x, $p) isa recommendation; select $p, $c";
        MatchQuery query2 = qb.parseMatch(queryString2).getMatchQuery();

        String explicitQuery = "match $p isa product;\n" +
                "$x isa person;$x id 'Karl Fischer';{($x, $p) isa recommendation} or" +
                "{$x isa person;$tt isa tag;$tt id 'Johann Wolfgang von Goethe';($x, $tt) isa tagging;$p isa product;$p id 'Faust'} or" +
                "{$x isa person; $p isa product;$p id 'Nocturnes'; $tt isa tag; ($tt, $x), isa tagging};" +
                "($p, $t) isa typing; select $p, $t";

        String explicitQuery2 = "match $p isa product;\n" +
                "$x isa person;$x id 'Karl Fischer';{($x, $p) isa recommendation} or" +
                "{$x isa person;$t isa tag, id 'Johann Wolfgang von Goethe';($x, $t) isa tagging;$p isa product;$p id 'Faust'} or" +
                "{$x isa person; $p isa product;$p id 'Nocturnes'; $t isa tag; ($t, $x), isa tagging};" +
                "($p, $c) isa typing; select $p, $c";

        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query2), Sets.newHashSet(qb.parseMatch(explicitQuery2).getMatchQuery()));
    }

    /**
     * Tests Bug #7422
     */
    @Test
    public void testInverseVars() {
        //select recommendation of Karl Fischer and their types
        String queryString = "match $p isa product;\n" +
                "$x isa person;$x id 'Karl Fischer'; ($p, $x) isa recommendation; ($p, $t) isa typing; select $p, $t";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();

        String explicitQuery = "match $p isa product;" +
                "$x isa person;$x id 'Karl Fischer';{($x, $p) isa recommendation} or" +
                "{$x isa person; $p isa product;$p id 'Nocturnes'; $tt isa tag; ($tt, $x), isa tagging} or" +
                "{$x isa person;$tt isa tag;$tt id 'Johann Wolfgang von Goethe';($x, $tt) isa tagging;$p isa product;$p id 'Faust'}" +
                ";($p, $t) isa typing; select $p, $t";

        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
    }

    @Test
    public void testDoubleVars() {

        String queryString = "match $x isa person;{($x, $y) isa recommendation} or " +
                "{" +
                "$x isa person;$t isa tag, id 'Enter_the_Chicken';" +
                "($x, $t) isa tagging;$y isa tag;{$y id 'Buckethead'} or {$y id 'Primus'}" +
                "} select $x, $y";

        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expand(query);
    }
    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
