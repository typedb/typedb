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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.inference;

import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.SNBKB;
import java.util.List;
import java.util.stream.Collectors;

import ai.grakn.util.GraknTestUtil;
import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static ai.grakn.util.GraqlTestUtil.assertCollectionsEqual;
import static ai.grakn.util.GraqlTestUtil.assertQueriesEqual;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class SNBInferenceTest {

    @Rule
    public final SampleKBContext snbGraph = SNBKB.context();

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(GraknTestUtil.usingTinker());
    }

    /**
     * Tests transitivity
     */
    @Test
    public void testTransitivity() {
        QueryBuilder qb = snbGraph.tx().graql().infer(false);
        QueryBuilder iqb = snbGraph.tx().graql().infer(true);
        String queryString = "match " +
                "$x isa university;$y isa country;(located-subject: $x, subject-location: $y) isa resides; get;";
        
        String explicitQuery = "match $x isa university, has name 'University of Cambridge';" +
                "$y isa country, has name 'UK'; get;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testTransitivityPrime() {
        QueryBuilder qb = snbGraph.tx().graql().infer(false);
        QueryBuilder iqb = snbGraph.tx().graql().infer(true);
        String queryString = "match " +
                "$x isa university;$y isa country;($x, $y) isa resides; get;";
        
        String explicitQuery = "match $x isa university, has name 'University of Cambridge';" +
                "$y isa country, has name 'UK'; get;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**
     * Tests transitivity
     */
    @Test
    public void testTransitivity2() {
        QueryBuilder qb = snbGraph.tx().graql().infer(false);
        QueryBuilder iqb = snbGraph.tx().graql().infer(true);
        String queryString = "match $x isa company;$y isa country;" +
                "(located-subject: $x, subject-location: $y) isa resides; get;";
        
        String explicitQuery = "match " +
                "$x isa company, has name 'Grakn';" +
                "$y isa country, has name 'UK'; get;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testTransitivity2Prime() {
        QueryBuilder qb = snbGraph.tx().graql().infer(false);
        QueryBuilder iqb = snbGraph.tx().graql().infer(true);
        String queryString = "match $x isa company;$y isa country;" +
                "($x, $y) isa resides; get;";
        
        String explicitQuery = "match " +
                "$x isa company, has name 'Grakn';" +
                "$y isa country, has name 'UK'; get;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testRecommendation() {
        QueryBuilder qb = snbGraph.tx().graql().infer(false);
        QueryBuilder iqb = snbGraph.tx().graql().infer(true);
        String queryString = "match $x isa person;($x, $y) isa recommendation; get;";
        String limitedQueryString = "match $x isa person;($x, $y) isa recommendation; limit 1; get;";
        GetQuery query = iqb.parse(queryString);
        GetQuery limitedQuery = iqb.parse(limitedQueryString);

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
                "{$x has name 'Gary';$y has name 'Pink Floyd';}; get;";

        long startTime = System.nanoTime();
        List<Answer> limitedAnswers = limitedQuery.execute();
        System.out.println("limited time: " + (System.nanoTime() - startTime)/1e6);

        startTime = System.nanoTime();
        List<Answer> answers = query.execute();
        System.out.println("full time: " + (System.nanoTime()- startTime)/1e6);
        assertCollectionsEqual(answers, qb.<GetQuery>parse(explicitQuery).execute());
        assertTrue(answers.containsAll(limitedAnswers));
    }

    /**
     * Tests relation filtering and rel vars matching
     */

    @Test
    public void testTag() {
        QueryBuilder qb = snbGraph.tx().graql().infer(false);
        QueryBuilder iqb = snbGraph.tx().graql().infer(true);
        String queryString = "match " +
                "$y isa person;$t isa tag;($y, $t) isa recommendation; get $y, $t;";
        
        String explicitQuery = "match $y isa person, has name $yName;$t isa tag, has name $tName;" +
                "{$yName val 'Charlie';" +
                "{$tName val 'Yngwie Malmsteen';} or {$tName val 'Cacophony';} or" +
                "{$tName val 'Steve Vai';} or {$tName val 'Black Sabbath';};} or " +
                "{$yName val 'Gary';$tName val 'Pink Floyd';};get $y, $t;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**
     * Tests relation filtering and rel vars matching
     */
    @Test
    public void testProduct() {
        QueryBuilder qb = snbGraph.tx().graql().infer(false);
        QueryBuilder iqb = snbGraph.tx().graql().infer(true);
        String queryString = "match " +
                "$y isa person;$yy isa product;($y, $yy) isa recommendation; get;";
        
        String explicitQuery = "match $y isa person, has name $ny; $yy isa product, has name $nyy;" +
                "{$ny val 'Alice';$nyy val 'War of the Worlds';} or" +
                "{$ny val 'Bob';{$nyy val 'Ducatti 1299';} or {$nyy val 'The Good the Bad the Ugly';};} or" +
                "{$ny val 'Charlie';{$nyy val 'Blizzard of Ozz';} or {$nyy val 'Stratocaster';};} or " +
                "{$ny val 'Denis';{$nyy val 'Colour of Magic';} or {$nyy val 'Dorian Gray';};} or"+
                "{$ny val 'Frank';$nyy val 'Nocturnes';} or" +
                "{$ny val 'Karl Fischer';{$nyy val 'Faust';} or {$nyy val 'Nocturnes';};} or " +
                "{$ny val 'Gary';$nyy val 'The Wall';};get $y, $yy;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testCombinedProductTag() {
        QueryBuilder qb = snbGraph.tx().graql().infer(false);
        QueryBuilder iqb = snbGraph.tx().graql().infer(true);
        String queryString = "match " +
                "{$p isa person;$r isa product;($p, $r) isa recommendation;} or" +
                "{$p isa person;$r isa tag;($p, $r) isa recommendation;}; get;";
        
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
                "{$p has name 'Gary';$r has name 'Pink Floyd';}; get;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testBook() {
        QueryBuilder qb = snbGraph.tx().graql().infer(false);
        QueryBuilder iqb = snbGraph.tx().graql().infer(true);
        String queryString = "match $x isa person;" +
                "($x, $y) isa recommendation;" +
                "$c isa category;$c has name 'book';" +
                "($y, $c) isa typing;" +
                "get $x, $y;";
        
        String explicitQuery = "match $x isa person, has name $nx;$y isa product, has name $ny;" +
                "{$nx val 'Alice';$ny val 'War of the Worlds';} or" +
                "{$nx val 'Karl Fischer';$ny val 'Faust';} or " +
                "{$nx val 'Denis';{$ny val 'Colour of Magic';} or {$ny val 'Dorian Gray';};};get $x, $y;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testBand() {
        QueryBuilder qb = snbGraph.tx().graql().infer(false);
        QueryBuilder iqb = snbGraph.tx().graql().infer(true);
        String queryString = "match $x isa person;" +
                "($x, $y) isa recommendation;" +
                "$c isa category;$c has name 'Band';" +
                "($y, $c) isa grouping;" +
                "get $x, $y;";
        
        String explicitQuery = "match " +
                "{$x has name 'Charlie';{$y has name 'Cacophony';} or {$y has name 'Black Sabbath';};} or " +
                "{$x has name 'Gary';$y has name 'Pink Floyd';}; get $x, $y;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**
     * Tests global variable consistency (Bug #7344)
     */
    @Test
    public void testVarConsistency(){
        QueryBuilder qb = snbGraph.tx().graql().infer(false);
        QueryBuilder iqb = snbGraph.tx().graql().infer(true);
        String queryString = "match $x isa person;$y isa product;" +
                "($x, $y) isa recommendation;" +
                "$z isa category;$z has name 'motorbike';" +
                "($y, $z) isa typing; get $x, $y;";

        String explicitQuery = "match $x isa person;$y isa product;" +
                "{$x has name 'Bob';$y has name 'Ducatti 1299';}; get;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**
     * tests whether rules are filtered correctly (rules recommending products other than Chopin should not be attached)
     */
    @Test
    public void testVarConsistency2(){
        QueryBuilder qb = snbGraph.tx().graql().infer(false);
        QueryBuilder iqb = snbGraph.tx().graql().infer(true);
        //select people that have Chopin as a recommendation
        String queryString = "match $x isa person; $y isa tag; ($x, $y) isa tagging;" +
                "$z isa product;$z has name 'Nocturnes'; ($x, $z) isa recommendation; get $x, $y;";


        String explicitQuery = "match " +
                "{$x has name 'Frank';$y has name 'Ludwig van Beethoven';} or" +
                "{$x has name 'Karl Fischer';" +
                "{$y has name 'Ludwig van Beethoven';} or {$y has name 'Johann Wolfgang von Goethe';} or" +
                "{$y has name 'Wolfgang Amadeus Mozart';};}; get $x, $y;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testVarConsistency3(){
        QueryBuilder qb = snbGraph.tx().graql().infer(false);
        QueryBuilder iqb = snbGraph.tx().graql().infer(true);
        String queryString = "match $x isa person;$pr isa product, has name 'Nocturnes';($x, $pr) isa recommendation; get $x;";
        String explicitQuery = "match {$x has name 'Frank';} or {$x has name 'Karl Fischer';}; get $x;";
        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**
     * Tests transitivity and Bug #7416
     */
    @Test
    public void testQueryConsistency() {
        QueryBuilder iqb = snbGraph.tx().graql().infer(true);
        String queryString = "match $x isa person; $y isa place; ($x, $y) isa resides;" +
                        "$z isa person;$z has name 'Miguel Gonzalez'; ($x, $z) isa knows; get $x, $y;";
        
        String queryString2 = "match $x isa person; $y isa person;$y has name 'Miguel Gonzalez';" +
                        "$z isa place; ($x, $y) isa knows; ($x, $z) isa resides; get $x, $z;";
        Unifier unifier = new UnifierImpl(ImmutableMap.of(Graql.var("z"), Graql.var("y")));

        List<Answer> answers = iqb.materialise(false).<GetQuery>parse(queryString).execute();
        List<Answer> answers2 =  iqb.materialise(false).<GetQuery>parse(queryString2).execute().stream().map(a -> a.unify(unifier)).collect(Collectors.toList());
        assertCollectionsEqual(answers, answers2);
    }

    /**
     * Tests Bug #7416
     * the $t variable in the query matches with $t from rules so if the rule var is not changed an extra condition is created
     * which renders the query unsatisfiable
     */
    @Test
    public void testOrdering() {
        QueryBuilder qb = snbGraph.tx().graql().infer(false);
        QueryBuilder iqb = snbGraph.tx().graql().infer(true);
        //select recommendationS of Karl Fischer and their types
        String queryString = "match $p isa product;$x isa person;$x has name 'Karl Fischer';" +
                        "($x, $p) isa recommendation; ($p, $t) isa typing; get $p, $t;";

        String explicitQuery = "match $p isa product;" +
                "$x isa person;$x has name 'Karl Fischer';{($x, $p) isa recommendation;} or" +
                "{$x isa person;$tt isa tag;$tt has name 'Johann Wolfgang von Goethe';" +
                "($x, $tt) isa tagging;$p isa product;$p has name 'Faust';} or" +
                "{$x isa person; $p isa product;$p has name 'Nocturnes'; $tt isa tag; ($tt, $x), isa tagging;};" +
                "($p, $t) isa typing; get $p, $t;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**
     * Tests Bug #7422
     */
    @Test
    public void testInverseVars() {
        QueryBuilder qb = snbGraph.tx().graql().infer(false);
        QueryBuilder iqb = snbGraph.tx().graql().infer(true);
        //select recommendation of Karl Fischer and their types
        String queryString = "match $p isa product;" +
                "$x isa person;$x has name 'Karl Fischer'; ($p, $x) isa recommendation; ($p, $t) isa typing; get $p, $t;";
        
        String explicitQuery = "match $p isa product;" +
                "$x isa person;$x has name 'Karl Fischer';{($x, $p) isa recommendation;} or" +
                "{$x isa person; $p isa product;$p has name 'Nocturnes'; $tt isa tag; ($tt, $x), isa tagging;} or" +
                "{$x isa person;$tt isa tag;$tt has name 'Johann Wolfgang von Goethe';($x, $tt) isa tagging;" +
                "$p isa product;$p has name 'Faust';};" +
                "($p, $t) isa typing; get $p, $t;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }
}
