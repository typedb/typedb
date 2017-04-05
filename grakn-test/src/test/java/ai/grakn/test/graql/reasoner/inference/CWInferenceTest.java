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
import ai.grakn.concept.RuleType;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.reasoner.Reasoner;
import ai.grakn.graphs.CWGraph;
import ai.grakn.test.GraphContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.and;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import static ai.grakn.test.GraknTestEnv.*;

public class CWInferenceTest {
    private static QueryBuilder qb;
    private static QueryBuilder iqb;

    @ClassRule
    public static GraphContext cwGraph = GraphContext.preLoad(CWGraph.get());

    @ClassRule
    public static GraphContext cwGraph2 = GraphContext.preLoad(CWGraph.get());

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(usingTinker());
        qb = cwGraph.graph().graql().infer(false);
        iqb = cwGraph.graph().graql().infer(true).materialise(false);
    }

    @Test
    public void testWeapon() {
        String queryString = "match $x isa weapon;";
        String explicitQuery = "match " +
                "{$x isa weapon;} or {" +
                "{{$x isa missile;} or {$x isa rocket;$x has propulsion 'gsp';};} or {$x isa rocket;$x has propulsion 'gsp';};" +
                "};";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testAlignment() {
        String queryString = "match $z isa country;$z has alignment 'hostile';";
        String explicitQuery = "match $z isa country, has name 'Nono';";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testTransactionQuery() {
        QueryBuilder qb = cwGraph2.graph().graql().infer(false);
                String queryString = "match $x isa person;$z isa country;($x, $y, $z) isa transaction;";
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
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testTransactionQuery2() {
        String queryString = "match $x isa person;$z isa country;$y isa weapon;($x, $y, $z) isa transaction;";
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
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testQuery() {
        String queryString = "match $x isa criminal;";
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
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testQueryWithOr() {
        String queryString = "match {$x isa criminal;} or {$x has nationality 'American';$x isa person;};";
        String explicitQuery = "match " +
            "{{$x isa criminal;} or {$x has nationality 'American';" +
            "{$z has alignment 'hostile';} or {" +
                "$yy val 'America';" +
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
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testVarSub() {
        String queryString = "match" +
                "$y isa person;$yy isa country;$yyy isa weapon;" +
                "($y, $yy, $yyy) isa transaction;";
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
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testVarSub2() {
        String queryString = "match" +
                "$y isa person;$z isa country;$x isa weapon;" +
                "($y, $z, $x) isa transaction;";
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
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testGraphCase() {
        GraknGraph localGraph = cwGraph2.graph();
        QueryBuilder lqb = localGraph.graql().infer(false);
        QueryBuilder ilqb = localGraph.graql().infer(true);
        RuleType inferenceRule = localGraph.getRuleType("inference-rule");

        localGraph.putEntityType("region");

        Pattern R6_LHS = and(localGraph.graql().parsePatterns("$x isa region;"));
        Pattern R6_RHS = and(localGraph.graql().parsePatterns("$x isa country;"));
        inferenceRule.putRule(R6_LHS, R6_RHS);
        Reasoner.commitGraph(localGraph);

        String queryString = "match $x isa criminal;";
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

        cwGraph2.graph(); //Reopen transaction
        assertQueriesEqual(ilqb.parse(queryString), lqb.parse(explicitQuery));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(q1.stream().collect(Collectors.toSet()), q2.stream().collect(Collectors.toSet()));
    }
}
