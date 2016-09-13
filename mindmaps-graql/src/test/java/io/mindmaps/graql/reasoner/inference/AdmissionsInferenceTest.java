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

package io.mindmaps.graql.reasoner.inference;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.graql.reasoner.graphs.AdmissionsGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class AdmissionsInferenceTest {

    private static MindmapsGraph graph;
    private static Reasoner reasoner;
    private static QueryBuilder qb;

    @BeforeClass
    public static void setUpClass() {
        graph = AdmissionsGraph.getGraph();
        reasoner = new Reasoner(graph);
        qb = Graql.withGraph(graph);
    }

    @Test
    public void testConditionalAdmission() {
        String queryString = "match $x isa applicant; $x has admissionStatus 'conditional'";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();
        String explicitQuery = "match $x isa applicant, id 'Bob'";
        assertQueriesEqual(reasoner.expand(query), qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
    }

    @Test
    public void testDeniedAdmission() {
        String queryString = "match $x isa applicant;$x has admissionStatus 'denied'";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();
        String explicitQuery = "match $x isa applicant, id 'Alice'";
        MatchQuery expandedQuery = reasoner.expand(query);
        assertQueriesEqual(expandedQuery, qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
    }

    @Test
    public void testProvisionalAdmission() {
        String queryString = "match $x isa applicant;$x has admissionStatus 'provisional'";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();
        String explicitQuery = "match $x isa applicant, id 'Denis'";
        MatchQuery expandedQuery = reasoner.expand(query);
        assertQueriesEqual(expandedQuery, qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
    }

    @Test
    public void testWaitForTranscriptAdmission() {
        String queryString = "match $x isa applicant;$x has admissionStatus 'wait for transcript'";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();
        String explicitQuery = "match $x isa applicant, id 'Frank'";
        MatchQuery expandedQuery = reasoner.expand(query);
        assertQueriesEqual(expandedQuery, qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
    }

    @Test
    public void testFullStatusAdmission() {
        String queryString = "match $x isa applicant;$x has admissionStatus 'full'";
        MatchQuery query = qb.parseMatch(queryString).getMatchQuery();
        String explicitQuery = "match $x isa applicant; $x id 'Eva' or $x id 'Charlie'";
        MatchQuery expandedQuery = reasoner.expand(query);
        assertQueriesEqual(expandedQuery, qb.parseMatch(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery).getMatchQuery()));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
