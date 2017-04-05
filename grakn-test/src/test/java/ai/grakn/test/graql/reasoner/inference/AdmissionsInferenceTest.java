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

import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graphs.AdmissionsGraph;
import ai.grakn.test.GraphContext;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.util.stream.Collectors;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;


public class AdmissionsInferenceTest {

    @Rule
    public final GraphContext admissionsGraph = GraphContext.preLoad(AdmissionsGraph.get());

    @BeforeClass
    public static void onStartup(){
        assumeTrue(usingTinker());
    }

    @Test
    public void testConditionalAdmission() {
        QueryBuilder qb = admissionsGraph.graph().graql().infer(false);
        QueryBuilder iqb = admissionsGraph.graph().graql().infer(true);

        String queryString = "match $x isa applicant;$x has admissionStatus 'conditional';";
        String explicitQuery = "match $x isa applicant, has name 'Bob';";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testDeniedAdmission() {
        QueryBuilder qb = admissionsGraph.graph().graql().infer(false);
        QueryBuilder iqb = admissionsGraph.graph().graql().infer(true);

        String queryString = "match $x isa applicant;$x has admissionStatus 'denied';";
        String explicitQuery = "match $x isa applicant, has name 'Alice';";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testProvisionalAdmission() {
        QueryBuilder qb = admissionsGraph.graph().graql().infer(false);
        QueryBuilder iqb = admissionsGraph.graph().graql().infer(true);

        String queryString = "match $x isa applicant;$x has admissionStatus 'provisional';";
        String explicitQuery = "match $x isa applicant, has name 'Denis';";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testWaitForTranscriptAdmission() {
        QueryBuilder qb = admissionsGraph.graph().graql().infer(false);
        QueryBuilder iqb = admissionsGraph.graph().graql().infer(true);

        String queryString = "match $x isa applicant;$x has admissionStatus 'wait for transcript';";
        String explicitQuery = "match $x isa applicant, has name 'Frank';";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testFullStatusAdmission() {
        QueryBuilder qb = admissionsGraph.graph().graql().infer(false);
        QueryBuilder iqb = admissionsGraph.graph().graql().infer(true);

        String queryString = "match $x isa applicant;$x has admissionStatus 'full';";
        String explicitQuery = "match $x isa applicant, has name $name;{$name val 'Charlie';} or {$name val 'Eva';};select $x;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testAdmissions() {
        QueryBuilder qb = admissionsGraph.graph().graql().infer(false);
        QueryBuilder iqb = admissionsGraph.graph().graql().infer(true);

        String queryString = "match $x has admissionStatus $y;$x has name $name;";
        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(queryString));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(queryString));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(q1.stream().collect(Collectors.toSet()), q2.stream().collect(Collectors.toSet()));
    }
}
