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
import ai.grakn.concept.RuleType;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Reasoner;
import ai.grakn.test.graql.reasoner.graphs.CWGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static ai.grakn.graql.Graql.and;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;


public class CWInferenceTest extends AbstractEngineTest{

    @BeforeClass
    public static void onStartup(){
        assumeTrue(usingTinker());
    }

    @Test
    public void testWeapon() {
        GraknGraph graph = CWGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $x isa weapon;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "{$x isa weapon;} or {" +
                "{{$x isa missile;} or {$x isa rocket;$x has propulsion 'gsp';};} or {$x isa rocket;$x has propulsion 'gsp';};" +
                "};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testAlignment() {
        GraknGraph graph = CWGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $z isa country;$z has alignment 'hostile';";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $z isa country, has name 'Nono';";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testTransactionQuery() {
        GraknGraph graph = CWGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $x isa person;$z isa country;($x, $y, $z) isa transaction;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$x isa person;" +
                "$z isa country;" +
                "{($x, $y, $z) isa transaction;} or {" +
                "$x isa person;" +
                "$z isa country;" +
                "{{$y isa weapon;} or {" +
                "{{$y isa missile;} or {$y isa rocket;$y has propulsion 'gsp';};} or {$y isa rocket;$y has propulsion 'gsp';};" +
                "};} or {{$y isa missile;} or {$y isa rocket;$y has propulsion 'gsp';};};" +
                "($x, $z) isa is-paid-by;" +
                "($z, $y) isa owns;" +
                "};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testTransactionQuery2() {
        GraknGraph graph = CWGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $x isa person;$z isa country;$y isa weapon;($x, $y, $z) isa transaction;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$x isa person;" +
                "$z isa country;" +
                "{$y isa weapon;} or {" +
                "{{$y isa missile;} or {$y isa rocket;$y has propulsion 'gsp';};} or {$y isa rocket;$y has propulsion 'gsp';};" +
                "};" +
                "{($x, $y, $z) isa transaction;} or {" +
                "$x isa person;" +
                "$z isa country;" +
                "{{$y isa weapon;} or {" +
                "{{$y isa missile;} or {$y isa rocket;$y has propulsion 'gsp';};} or {$y isa rocket;$y has propulsion 'gsp';};" +
                "};} or {{$y isa missile;} or {$y isa rocket;$y has propulsion 'gsp';};};" +
                "($x, $z) isa is-paid-by;" +
                "($z, $y) isa owns;" +
                "};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testQuery() {
        GraknGraph graph = CWGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match $x isa criminal;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "{$x isa criminal;} or {" +
                "$x has nationality 'American';" +
                "($x, $y, $z) isa transaction or {" +
                    "$x isa person;$z isa country;" +
                    "{ {$y isa weapon;} or { {$y isa missile;} or {$y isa rocket;$y has propulsion 'gsp';}; }; };" +
                    "($x, $z) isa is-paid-by;($z, $y) isa owns;" +
                    "};" +
                "{$y isa weapon;} or {$y isa missile;} or {$y has propulsion 'gsp';$y isa rocket;};" +
                "{$z has alignment 'hostile';} or {" +
                    "$y1 isa country;$y1 has name 'America';" +
                    "($z, $y1) isa is-enemy-of;" +
                    "$z isa country;" +
                    "};" +
                "$x isa person;" +
                "$z isa country;" +
                "}; select $x;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testQueryWithOr() {
        GraknGraph graph = CWGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match {$x isa criminal;} or {$x has nationality 'American';$x isa person;};";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
            "{{$x isa criminal;} or {$x has nationality 'American';" +
            "{$z has alignment 'hostile';} or {" +
                "$yy value 'America';" +
                "($z, $yy) isa is-enemy-of;" +
                "$z isa country;" +
                "$yy isa country;" +
            "};" +
            "($x, $y, $z) isa transaction or {" +
                "$x isa person;" +
                "$z isa country;" +
                "{ {$y isa weapon;} or { {$y isa missile;} or {$y isa rocket;$y has propulsion 'gsp';} ;} ;};" +
                "($x, $z) isa is-paid-by;" +
                "($z, $y) isa owns;" +
            "};" +
            "{$y isa weapon;} or {{$y isa missile;} or {$y has propulsion 'gsp';$y isa rocket;};};" +
            "$x isa person;" +
            "$z isa country;};} or {$x has nationality 'American';$x isa person;}; select $x;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    public void testVarSub() {
        GraknGraph graph = CWGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match" +
                "$y isa person;$yy isa country;$yyy isa weapon;" +
                "($y, $yy, $yyy) isa transaction;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$y isa person;" +
                "$yy isa country;" +
                "{$yyy isa weapon;} or {" +
                "{{$yyy isa missile;} or {$yyy isa rocket;$yyy has propulsion 'gsp';};} or {$yyy isa rocket;$yyy has propulsion 'gsp';};" +
                "};" +
                "{($y, $yy, $yyy) isa transaction;} or {" +
                "$y isa person;" +
                "$yy isa country;" +
                "{{$yyy isa weapon;} or {" +
                "{{$yyy isa missile;} or {$yyy isa rocket;$yyy has propulsion 'gsp';};} or {$yyy isa rocket;$yyy has propulsion 'gsp';};" +
                "};} or {{$yyy isa missile;} or {$yyy isa rocket;$yyy has propulsion 'gsp';};};" +
                "($y, $yy) isa is-paid-by;" +
                "($yy, $yyy) isa owns;" +
                "};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testVarSub2() {
        GraknGraph graph = CWGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);
        String queryString = "match" +
                "$y isa person;$z isa country;$x isa weapon;" +
                "($y, $z, $x) isa transaction;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match " +
                "$y isa person;" +
                "$z isa country;" +
                "{$x isa weapon;} or {" +
                "{{$x isa missile;} or {$x isa rocket;$x has propulsion 'gsp';};} or {$x isa rocket;$x has propulsion 'gsp';};" +
                "};" +
                "{($y, $z, $x) isa transaction;} or {" +
                "$y isa person;" +
                "$z isa country;" +
                "{{$x isa weapon;} or {" +
                "{{$x isa missile;} or {$x isa rocket;$x has propulsion 'gsp';};} or {$x isa rocket;$x has propulsion 'gsp';};" +
                "};} or {{$x isa missile;} or {$x isa rocket;$x has propulsion 'gsp';};};" +
                "($y, $z) isa is-paid-by;" +
                "($z, $x) isa owns;" +
                "};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testGraphCase() {
        GraknGraph localGraph = CWGraph.getGraph();
        Reasoner localReasoner = new Reasoner(localGraph);
        QueryBuilder lqb = localGraph.graql();
        RuleType inferenceRule = localGraph.getRuleType("inference-rule");

        localGraph.putEntityType("region");

        Pattern R6_LHS = and(localGraph.graql().parsePatterns("$x isa region;"));
        Pattern R6_RHS = and(localGraph.graql().parsePatterns("$x isa country;"));
        inferenceRule.addRule(R6_LHS, R6_RHS);

        localReasoner.linkConceptTypes();
        String queryString = "match $x isa criminal;";
        MatchQuery query = lqb.parse(queryString);

        String explicitQuery = "match " +
                "{$x isa criminal;} or {" +
                "$x has nationality 'American';" +
                "($x, $y, $z) isa transaction or {" +
                "$x isa person ;" +
                "{$z isa country;} or {$z isa region;};" +
                "{ {$y isa weapon;} or { {$y isa missile;} or {$y isa rocket;$y has propulsion 'gsp';}; }; };" +
                "($x, $z) isa is-paid-by;" +
                "($z, $y) isa owns;" +
                "};" +
                "{$y isa weapon;} or {{$y isa missile;} or {$y has propulsion 'gsp';$y isa rocket;};};" +
                "{$z has alignment 'hostile';} or {" +
                "$yy has name 'America';" +
                "($z, $yy) isa is-enemy-of;" +
                "$z isa country;" +
                "$yy isa country;" +
                "};" +
                "}; select $x;";

        assertEquals(localReasoner.resolve(query), Sets.newHashSet(lqb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), lqb.parseMatch(explicitQuery));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
