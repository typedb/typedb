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

package io.mindmaps.reasoner.inference;

import com.google.common.collect.Sets;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.reasoner.MindmapsReasoner;
import io.mindmaps.reasoner.graphs.AdmissionsGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class AdmissionsInferenceTest {

    private static MindmapsTransaction graph;
    private static MindmapsReasoner reasoner;
    private static QueryParser qp;

    @BeforeClass
    public static void setUpClass() {

        graph = AdmissionsGraph.getTransaction();
        reasoner = new MindmapsReasoner(graph);
        qp = QueryParser.create(graph);

    }

    @Test
    public void testConditionalAdmission()
    {
        String queryString = "match $x isa applicant; $x has admissionStatus 'conditional'";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery("match $x isa applicant, value 'Bob'").getMatchQuery());

    }

    @Test
    public void testDeniedAdmission()
    {
        String queryString = "match $x isa applicant;$x has admissionStatus 'denied'";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery("match $x isa applicant, value 'Alice'").getMatchQuery());

    }

    @Test
    public void testProvisionalAdmission()
    {
        String queryString = "match $x isa applicant;$x has admissionStatus 'provisional'";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery("match $x isa applicant, value 'Denis'").getMatchQuery());
    }

    @Test
    public void testWaitForTranscriptAdmission()
    {
        String queryString = "match $x isa applicant;$x has admissionStatus 'wait for transcript'";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery("match $x isa applicant, value 'Frank'").getMatchQuery());
    }

    @Test
    public void testFullStatusAdmission()
    {
        String queryString = "match $x isa applicant;$x has admissionStatus 'full'";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery("match $x isa applicant; $x value 'Eva' or $x value 'Charlie'").getMatchQuery());
    }


    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
