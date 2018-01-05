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

import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.AdmissionsKB;
import ai.grakn.util.GraknTestUtil;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static ai.grakn.util.GraqlTestUtil.assertQueriesEqual;
import static org.junit.Assume.assumeTrue;


public class AdmissionsInferenceTest {

    @Rule
    public final SampleKBContext admissionsKB = AdmissionsKB.context();

    @BeforeClass
    public static void onStartup(){
        assumeTrue(GraknTestUtil.usingTinker());
    }

    @Test
    public void testConditionalAdmission() {
        QueryBuilder qb = admissionsKB.tx().graql().infer(false);
        QueryBuilder iqb = admissionsKB.tx().graql().infer(true);

        String queryString = "match $x isa applicant;$x has admissionStatus 'conditional'; get;";
        String explicitQuery = "match $x isa applicant, has name 'Bob'; get;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testDeniedAdmission() {
        QueryBuilder qb = admissionsKB.tx().graql().infer(false);
        QueryBuilder iqb = admissionsKB.tx().graql().infer(true);

        String queryString = "match $x isa applicant;$x has admissionStatus 'denied'; get;";
        String explicitQuery = "match $x isa applicant, has name 'Alice'; get;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testProvisionalAdmission() {
        QueryBuilder qb = admissionsKB.tx().graql().infer(false);
        QueryBuilder iqb = admissionsKB.tx().graql().infer(true);

        String queryString = "match $x isa applicant;$x has admissionStatus 'provisional'; get;";
        String explicitQuery = "match $x isa applicant, has name 'Denis'; get;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testWaitForTranscriptAdmission() {
        QueryBuilder qb = admissionsKB.tx().graql().infer(false);
        QueryBuilder iqb = admissionsKB.tx().graql().infer(true);

        String queryString = "match $x isa applicant;$x has admissionStatus 'wait for transcript'; get;";
        String explicitQuery = "match $x isa applicant, has name 'Frank'; get;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testFullStatusAdmission() {
        QueryBuilder qb = admissionsKB.tx().graql().infer(false);
        QueryBuilder iqb = admissionsKB.tx().graql().infer(true);

        String queryString = "match $x isa applicant;$x has admissionStatus 'full'; get;";
        String explicitQuery = "match $x isa applicant, has name $name;{$name val 'Charlie';} or {$name val 'Eva';};get $x;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testAdmissions() {
        QueryBuilder qb = admissionsKB.tx().graql().infer(false);
        QueryBuilder iqb = admissionsKB.tx().graql().infer(true);

        String queryString = "match $x has admissionStatus $y;$x has name $name; get;";
        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(queryString));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(queryString));
    }
}
