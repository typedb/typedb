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
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.reasoner.MindmapsReasoner;
import io.mindmaps.reasoner.graphs.GeoGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.mindmaps.reasoner.internal.Utility.printMatchQueryResults;
import static org.junit.Assert.assertEquals;


public class GeoInferenceTest {

    private static MindmapsTransaction graph;
    private static MindmapsReasoner reasoner;
    private static QueryParser qp;

    @BeforeClass
    public static void setUpClass() {

        graph = GeoGraph.getTransaction();
        reasoner = new MindmapsReasoner(graph);
        qp = QueryParser.create(graph);
    }

    private static void printMatchQuery(MatchQuery query) {
        System.out.println(query.toString().replace(" or ", "\nor\n").replace("};", "};\n").replace("; {", ";\n{"));
    }


    @Test
    public void testQuery()
    {
        //show me all cities in Poland
        String queryString = "match " +
                        "$x isa city;\n" +
                        "(geo-entity $x, entity-location $y) isa is-located-in;\n"+
                        "$y isa country, value 'Poland'; select $x";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        printMatchQueryResults(query.distinct());
        MatchQuery expandedQuery = reasoner.expandQuery(query);
        printMatchQuery(expandedQuery);
        printMatchQueryResults(expandedQuery.distinct());


        String explicitQuery = "match " +
                "$x isa city;\n" +
                "(geo-entity $x, entity-location $y) isa is-located-in or"+
                "{(geo-entity $x, entity-location $z) isa is-located-in; (geo-entity $z, entity-location $y) isa is-located-in};\n" +
                "$y isa country;\n" +
                "$y value 'Poland'; select $x";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }

    @Test
    public void testQuery2()
    {
        //show me all universities in Poland
        String queryString = "match " +
                "$x isa university;\n" +
                "($x, $y) isa is-located-in;\n"+
                "$y isa country;\n" +
                "$y value 'Poland'; select $x";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);
        printMatchQuery(expandedQuery);
        printMatchQueryResults(expandedQuery.distinct());


        String explicitQuery = "match " +
                "$x isa university;\n" +
                "($x, $y) isa is-located-in or"+
                "{($x, $zz) isa is-located-in; ($zz, $z) isa is-located-in;($z, $y) isa is-located-in};\n" +
                "$y isa country;\n" +
                "$y value 'Poland'; select $x";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
