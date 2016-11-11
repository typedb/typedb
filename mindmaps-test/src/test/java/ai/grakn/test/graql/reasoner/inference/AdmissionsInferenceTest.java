/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

import ai.grakn.graql.Reasoner;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.test.graql.reasoner.graphs.AdmissionsGraph;
import com.google.common.collect.Sets;
import ai.grakn.MindmapsGraph;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Reasoner;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.test.graql.reasoner.graphs.AdmissionsGraph;
import org.junit.Test;

import static ai.grakn.graql.internal.reasoner.Utility.printAnswers;
import static org.junit.Assert.assertEquals;


public class AdmissionsInferenceTest {

    @Test
    public void testConditionalAdmission() {
        MindmapsGraph graph = AdmissionsGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match $x isa applicant;$x has admissionStatus 'conditional';";
        Query query = new Query(queryString, graph);
        String explicitQuery = "match $x isa applicant, has name 'Bob';";

        Utility.printAnswers(reasoner.resolve(query));
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        //assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testDeniedAdmission() {
        MindmapsGraph graph = AdmissionsGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match $x isa applicant;$x has admissionStatus 'denied';";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $x isa applicant, has name 'Alice';";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testProvisionalAdmission() {
        MindmapsGraph graph = AdmissionsGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match $x isa applicant;$x has admissionStatus 'provisional';";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $x isa applicant, has name 'Denis';";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testWaitForTranscriptAdmission() {
        MindmapsGraph graph = AdmissionsGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match $x isa applicant;$x has admissionStatus 'wait for transcript';";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $x isa applicant, has name 'Frank';";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testFullStatusAdmission() {
        MindmapsGraph graph = AdmissionsGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match $x isa applicant;$x has name $name;$x has admissionStatus 'full';";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $x isa applicant, has name $name;{$name value 'Charlie';} or {$name value 'Eva';};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(Sets.newHashSet(qb.<MatchQuery>parse(queryString)), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    public void testAdmissions() {
        MindmapsGraph graph = AdmissionsGraph.getGraph();
        QueryBuilder qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match $x has admissionStatus $y;$x has name $name;";
        Query query = new Query(queryString, graph);
        assertEquals(Sets.newHashSet(reasoner.resolve(query)), Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
