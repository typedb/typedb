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
import ai.grakn.test.kbs.AdmissionsKB;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.util.GraqlTestUtil.assertQueriesEqual;

public class AdmissionsInferenceTest {

    @ClassRule
    public static final SampleKBContext admissionsKB = AdmissionsKB.context();

    @Test
    public void testConditionalAdmission() {
        QueryBuilder qb = admissionsKB.tx().graql().infer(false);
        QueryBuilder iqb = admissionsKB.tx().graql().infer(true);

        String queryString = "match $x isa applicant;$x has admissionStatus 'conditional'; get;";
        String explicitQuery = "match $x isa applicant, has name 'Bob'; get;";

        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testDeniedAdmission() {
        QueryBuilder qb = admissionsKB.tx().graql().infer(false);
        QueryBuilder iqb = admissionsKB.tx().graql().infer(true);

        String queryString = "match $x isa applicant;$x has admissionStatus 'denied'; get;";
        String explicitQuery = "match $x isa applicant, has name 'Alice'; get;";

        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testProvisionalAdmission() {
        QueryBuilder qb = admissionsKB.tx().graql().infer(false);
        QueryBuilder iqb = admissionsKB.tx().graql().infer(true);

        String queryString = "match $x isa applicant;$x has admissionStatus 'provisional'; get;";
        String explicitQuery = "match $x isa applicant, has name 'Denis'; get;";

        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testWaitForTranscriptAdmission() {
        QueryBuilder qb = admissionsKB.tx().graql().infer(false);
        QueryBuilder iqb = admissionsKB.tx().graql().infer(true);

        String queryString = "match $x isa applicant;$x has admissionStatus 'wait for transcript'; get;";
        String explicitQuery = "match $x isa applicant, has name 'Frank'; get;";

        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testFullStatusAdmission() {
        QueryBuilder qb = admissionsKB.tx().graql().infer(false);
        QueryBuilder iqb = admissionsKB.tx().graql().infer(true);

        String queryString = "match $x isa applicant;$x has admissionStatus 'full'; get;";
        String explicitQuery = "match $x isa applicant, has name $name;{$name == 'Charlie';} or {$name == 'Eva';};get $x;";

        assertQueriesEqual(iqb.parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testAdmissions() {
        QueryBuilder qb = admissionsKB.tx().graql().infer(false);
        QueryBuilder iqb = admissionsKB.tx().graql().infer(true);

        String queryString = "match $x has admissionStatus $y;$x has name $name; get;";
        assertQueriesEqual(iqb.parse(queryString), qb.parse(queryString));
    }
}
