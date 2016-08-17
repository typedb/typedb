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
import io.mindmaps.graql.MindmapsReasoner;
import io.mindmaps.graql.reasoner.graphs.AbstractGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.mindmaps.graql.internal.reasoner.Utility.printMatchQueryResults;
import static org.junit.Assert.assertEquals;


public class AbstractInferenceTest {

    private static MindmapsReasoner reasoner;
    private static QueryParser qp;

    @BeforeClass
    public static void setUpClass() {

        MindmapsTransaction graph = AbstractGraph.getTransaction();
        reasoner = new MindmapsReasoner(graph);
        qp = QueryParser.create(graph);
    }


    @Test
    public void testQuery()
    {
        String queryString = "match $x isa Q;";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expQuery = reasoner.expandQuery(query);
        printMatchQueryResults(expQuery);

        String explicitQuery = "match " +
                "{$x isa Q} or {\n" +
                "{$y isa q} or {$y isa t};\n" +
                "{{$x isa p} or {$x isa s}} or {{$x isa r} or {$x isa u}};\n" +
                "($x, $y) isa rel\n" +
                "}; select $x";

        assertQueriesEqual(expQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    public void testQuery2()
    {
        String queryString = "match " +
                        "$yy isa Q;\n" +
                        "$y isa P;\n" +
                        "($y, $yy) isa REL; select $yy";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expQuery = reasoner.expandQuery(query);
        String expQueryString = expQuery.toString().replace(" or ", "\nor\n").replace("};", "};\n").replace("; {", ";\n{");
        System.out.println(expQueryString);
        printMatchQueryResults(expQuery);

        String explicitQuery = "match " +
                                "{$yy isa Q} or {" +
                                "{$yyy isa q} or {$yyy isa t};\n" +
                                "($yy, $yyy) isa rel;\n" +
                                "{{$yy isa p} or {$yy isa s}} or {{yy isa r} or {$yy isa u}}" +
                                "};" +
                                "$y isa P;\n" +
                                "($y, $yy) isa REL; select $yy";
        assertQueriesEqual(expQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    private void assertQueriesEqual(MatchQueryDefault q1, MatchQueryDefault q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }

}
