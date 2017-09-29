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

package ai.grakn.graql.internal.reasoner.inference;

import ai.grakn.GraknTx;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.CWKB;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.util.GraqlTestUtil.assertQueriesEqual;
import static org.junit.Assume.assumeTrue;

public class CWInferenceTest {
    private static QueryBuilder qb;
    private static QueryBuilder iqb;

    @ClassRule
    public static SampleKBContext cwKB = SampleKBContext.preLoad(CWKB.get()).assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static SampleKBContext cwKB2 = SampleKBContext.preLoad(CWKB.get()).assumeTrue(GraknTestSetup.usingTinker());

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(GraknTestSetup.usingTinker());
        qb = cwKB.tx().graql().infer(false);
        iqb = cwKB.tx().graql().infer(true).materialise(false);
    }

    @Test
    public void testWeapon() {
        String queryString = "match $x isa weapon; get;";
        String explicitQuery = "match " +
                "{$x isa weapon;} or {" +
                "{{$x isa missile;} or {$x isa rocket;$x has propulsion 'gsp';};} or {$x isa rocket;$x has propulsion 'gsp';};" +
                "}; get;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testAlignment() {
        String queryString = "match $z isa country;$z has alignment 'hostile'; get;";
        String explicitQuery = "match $z isa country, has name 'Nono'; get;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testTransactionQuery() {
        QueryBuilder qb = cwKB2.tx().graql().infer(false);
                String queryString = "match $x isa person;$z isa country;($x, $y, $z) isa transaction; get;";
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
                "}; get;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testTransactionQuery2() {
        String queryString = "match $x isa person;$z isa country;$y isa weapon;($x, $y, $z) isa transaction; get;";
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
                "}; get;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testQuery() {
        String queryString = "match $x isa criminal; get;";
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
                "}; get $x;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testQueryWithOr() {
        String queryString = "match {$x isa criminal;} or {$x has nationality 'American';$x isa person;}; get;";
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
            "$z isa country;};} or {$x has nationality 'American';$x isa person;}; get $x;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testVarSub() {
        String queryString = "match" +
                "$y isa person;$yy isa country;$yyy isa weapon;" +
                "($y, $yy, $yyy) isa transaction; get;";
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
                "}; get;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testVarSub2() {
        String queryString = "match" +
                "$y isa person;$z isa country;$x isa weapon;" +
                "($y, $z, $x) isa transaction; get;";
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
                "}; get;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testGraphCase() {
        GraknTx tx = cwKB2.tx();
        QueryBuilder lqb = tx.graql().infer(false);
        QueryBuilder ilqb = tx.graql().infer(true);

        tx.putEntityType("region");

        Pattern R6_LHS = and(tx.graql().parser().parsePatterns("$x isa region;"));
        Pattern R6_RHS = and(tx.graql().parser().parsePatterns("$x isa country;"));
        tx.putRule("R6: If something is a region it is a country", R6_LHS, R6_RHS);
        tx.admin().commitNoLogs();

        String queryString = "match $x isa criminal; get;";
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
                "}; get $x;";

        cwKB2.tx(); //Reopen transaction
        assertQueriesEqual(ilqb.parse(queryString), lqb.parse(explicitQuery));
    }
}
