/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.test.graql.reasoner.inference;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.graql.internal.reasoner.query.Query;
import io.mindmaps.test.graql.reasoner.graphs.AdmissionsGraph;
import org.junit.Test;

import static io.mindmaps.graql.internal.reasoner.Utility.printAnswers;
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

        printAnswers(reasoner.resolve(query));
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
