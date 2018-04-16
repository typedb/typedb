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

import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.kbs.CWKB;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.GraknTestUtil;
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
    public static SampleKBContext cwKB = CWKB.context();

    @ClassRule
    public static SampleKBContext cwKB2 = CWKB.context();

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(GraknTestUtil.usingTinker());
        qb = cwKB.tx().graql().infer(false);
        iqb = cwKB.tx().graql().infer(true).materialise(false);
    }

    @Test
    public void testWeapon() {
        String queryString = "match $w isa weapon; get;";
        String explicitQuery = "match $w has name 'Tomahawk'; get;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testAlignment() {
        String queryString = "match $k isa country;$k has alignment 'hostile'; get;";
        String explicitQuery = "match $k isa country, has name 'Nono'; get;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testTransactionQuery() {
        QueryBuilder qb = cwKB2.tx().graql().infer(false);
                String queryString = "match ($t1, $t2, $t3) isa transaction; get;";
        String explicitQuery = "match " +
                "{$t1 has name 'colonelWest';$t2 has name 'Tomahawk';$t3 has name 'Nono';} or " +
                "{$t1 has name 'colonelWest';$t3 has name 'Tomahawk';$t2 has name 'Nono';} or " +
                "{$t2 has name 'colonelWest';$t1 has name 'Tomahawk';$t3 has name 'Nono';} or " +
                "{$t2 has name 'colonelWest';$t3 has name 'Tomahawk';$t1 has name 'Nono';} or " +
                "{$t3 has name 'colonelWest';$t2 has name 'Tomahawk';$t1 has name 'Nono';} or " +
                "{$t3 has name 'colonelWest';$t1 has name 'Tomahawk';$t2 has name 'Nono';};" +
                "get;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testTypedTransactionQuery() {
        QueryBuilder qb = cwKB2.tx().graql().infer(false);
        String queryString = "match $p isa person;$w isa weapon;$c isa country;($p, $w, $c) isa transaction; get;";
        String explicitQuery = "match " +
                "$p isa person has name 'colonelWest';" +
                "$w has name 'Tomahawk';" +
                "$c has name 'Nono';" +
                "get;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testQuery() {
        String queryString = "match $p isa criminal; get;";
        String explicitQuery = "match $p isa person has name 'colonelWest'; get;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testGraphCase() {
        EmbeddedGraknTx<?> tx = cwKB2.tx();
        QueryBuilder lqb = tx.graql().infer(false);
        QueryBuilder ilqb = tx.graql().infer(true);

        tx.putEntityType("region");

        Pattern R6_LHS = and(tx.graql().parser().parsePatterns("$x isa region;"));
        Pattern R6_RHS = and(tx.graql().parser().parsePatterns("$x isa country;"));
        tx.putRule("R6: If something is a region it is a country", R6_LHS, R6_RHS);
        tx.commitSubmitNoLogs();

        String queryString = "match $p isa criminal; get;";
        String explicitQuery = "match $p isa person has name 'colonelWest'; get;";

        cwKB2.tx(); //Reopen transaction
        assertQueriesEqual(ilqb.parse(queryString), lqb.parse(explicitQuery));
    }
}
