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

import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.kbs.CWKB;
import ai.grakn.test.rule.SampleKBContext;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.util.GraqlTestUtil.assertQueriesEqual;

public class CWInferenceTest {

    @ClassRule
    public static SampleKBContext cwKB = CWKB.context();

    @ClassRule
    public static SampleKBContext cwFreshKB = CWKB.context();

    @Test
    public void testWeapon() {
        EmbeddedGraknTx<?> tx = cwKB.tx();
        QueryBuilder qb = tx.graql().infer(false);
        QueryBuilder iqb = tx.graql().infer(true);
        String queryString = "match $w isa weapon; get;";
        String explicitQuery = "match $w has name 'Tomahawk'; get;";
        assertQueriesEqual(iqb.infer(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testAlignment() {
        EmbeddedGraknTx<?> tx = cwKB.tx();
        QueryBuilder qb = tx.graql().infer(false);
        QueryBuilder iqb = tx.graql().infer(true);
        String queryString = "match $k isa country;$k has alignment 'hostile'; get;";
        String explicitQuery = "match $k isa country, has name 'Nono'; get;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testQuery() {
        EmbeddedGraknTx<?> tx = cwKB.tx();
        QueryBuilder qb = tx.graql().infer(false);
        QueryBuilder iqb = tx.graql().infer(true);
        String queryString = "match $p isa criminal; get;";
        String explicitQuery = "match $p isa person has name 'colonelWest'; get;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testGraphCase() {
        EmbeddedGraknTx<?> tx = cwFreshKB.tx();
        QueryBuilder qb = tx.graql().infer(false);
        QueryBuilder iqb = tx.graql().infer(true);

        tx.putEntityType("region");

        Pattern R6_LHS = and(tx.graql().parser().parsePatterns("$x isa region;"));
        Pattern R6_RHS = and(tx.graql().parser().parsePatterns("$x isa country;"));
        tx.putRule("R6: If something is a region it is a country", R6_LHS, R6_RHS);
        tx.commitSubmitNoLogs();

        String queryString = "match $p isa criminal; get;";
        String explicitQuery = "match $p isa person has name 'colonelWest'; get;";

        cwFreshKB.tx(); //Reopen transaction
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testTransactionQuery() {
        EmbeddedGraknTx<?> tx = cwFreshKB.tx();
        QueryBuilder qb = tx.graql().infer(false);
        QueryBuilder iqb = tx.graql().infer(true);
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
        EmbeddedGraknTx<?> tx = cwFreshKB.tx();
        QueryBuilder qb = tx.graql().infer(false);
        QueryBuilder iqb = tx.graql().infer(true);
        String queryString = "match $p isa person;$w isa weapon;$c isa country;($p, $w, $c) isa transaction; get;";
        String explicitQuery = "match " +
                "$p isa person has name 'colonelWest';" +
                "$w has name 'Tomahawk';" +
                "$c has name 'Nono';" +
                "get;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }
}
