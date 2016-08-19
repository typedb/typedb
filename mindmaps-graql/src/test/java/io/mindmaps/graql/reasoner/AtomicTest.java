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

package io.mindmaps.graql.reasoner;

import io.mindmaps.MindmapsTransaction;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQueryDefault;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.QueryParser;
import io.mindmaps.graql.internal.reasoner.container.Query;
import io.mindmaps.graql.reasoner.graphs.SNBGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.mindmaps.graql.internal.reasoner.Utility.printMatchQueryResults;

public class AtomicTest {
    private static MindmapsTransaction graph;
    private static QueryParser qp;
    private static QueryBuilder qb;

    @BeforeClass
    public static void setUpClass() {

        graph = SNBGraph.getTransaction();
        qp = QueryParser.create(graph);
        qb = Graql.withTransaction(graph);
    }

    @Test
    public void testValuePredicate(){

        String queryString = "match " +
                "$x1 isa person;\n" +
                "$x2 isa tag;\n" +
                "($x1, $x2) isa recommendation";

        Query query = new Query(queryString, graph);
        MatchQueryDefault MQ = qp.parseMatchQuery(queryString).getMatchQuery();
        printMatchQueryResults(MQ);

    }


}
