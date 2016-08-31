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
import io.mindmaps.MindmapsTransaction;
import io.mindmaps.graql.MatchQueryDefault;
import io.mindmaps.graql.QueryParser;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.graql.reasoner.graphs.AdmissionsGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class AdmissionsInferenceTest {

    private static MindmapsTransaction graph;
    private static Reasoner reasoner;
    private static QueryParser qp;

    @BeforeClass
    public static void setUpClass() {
        graph = AdmissionsGraph.getTransaction();
        reasoner = new Reasoner(graph);
        qp = QueryParser.create(graph);
    }

    @Test
    public void testConditionalAdmission() {
        String queryString = "match $x isa applicant; $x has admissionStatus 'conditional'";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery("match $x isa applicant, id 'Bob'").getMatchQuery());
    }

    @Test
    public void testDeniedAdmission() {
        String queryString = "match $x isa applicant;$x has admissionStatus 'denied'";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expandedQuery = reasoner.expand(query);
        assertQueriesEqual(expandedQuery, qp.parseMatchQuery("match $x isa applicant, id 'Alice'").getMatchQuery());
    }

    @Test
    public void testProvisionalAdmission() {
        String queryString = "match $x isa applicant;$x has admissionStatus 'provisional'";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expandedQuery = reasoner.expand(query);
        assertQueriesEqual(expandedQuery, qp.parseMatchQuery("match $x isa applicant, id 'Denis'").getMatchQuery());
    }

    @Test
    public void testWaitForTranscriptAdmission() {
        String queryString = "match $x isa applicant;$x has admissionStatus 'wait for transcript'";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expandedQuery = reasoner.expand(query);
        assertQueriesEqual(expandedQuery, qp.parseMatchQuery("match $x isa applicant, id 'Frank'").getMatchQuery());
    }

    @Test
    public void testFullStatusAdmission() {
        String queryString = "match $x isa applicant;$x has admissionStatus 'full'";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expandedQuery = reasoner.expand(query);
        assertQueriesEqual(expandedQuery, qp.parseMatchQuery("match $x isa applicant; $x id 'Eva' or $x id 'Charlie'").getMatchQuery());
    }

    private void assertQueriesEqual(MatchQueryDefault q1, MatchQueryDefault q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
