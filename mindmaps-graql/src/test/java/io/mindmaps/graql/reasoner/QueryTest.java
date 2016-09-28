/*
 * MindmapsDB  A Distributed Semantic Database
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

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;
import io.mindmaps.graql.internal.reasoner.query.Query;
import io.mindmaps.graql.internal.reasoner.query.QueryAnswers;
import io.mindmaps.graql.reasoner.graphs.SNBGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class QueryTest {

    private static MindmapsGraph graph;
    private static QueryBuilder qb;

    @BeforeClass
    public static void setUpClass() {

        graph = SNBGraph.getGraph();
        qb = Graql.withGraph(graph);
    }

    @Test
    public void testValuePredicate(){
        String queryString = "match $x isa person;$x value 'Bob';";

        Query query = new Query(queryString, graph);
        boolean containsAtom = false;
        for(Atomic atom : query.getAtoms())
            if (atom.toString().equals("$x value \"Bob\"")) containsAtom = true;
        assertTrue(containsAtom);
        assertEquals(query.getValue("x"), "Bob");
    }

    @Test
    public void testCopyConstructor(){
        String queryString = "match $x isa person;$y isa product;($x, $y) isa recommendation;";
        Query query = new Query(queryString, graph);
        Query copy = new Query(query);
        
        assertQueriesEqual(query.getMatchQuery(), copy.getMatchQuery());
    }

    @Test
    public void testTwinPattern() {
        String queryString = "match $x isa person;$x id 'Bob';";
        String queryString2 = "match $x isa person, id 'Bob';";
        String queryString3 = "match $x isa person, value 'Bob';";
        String queryString4 = "match $x isa person;$x value 'Bob';";

        Query query = new Query(queryString, graph);
        Query query2 = new Query(queryString2, graph);
        Query query3 = new Query(queryString3, graph);
        Query query4 = new Query(queryString4, graph);

        assertTrue(query.isEquivalent(query2));
        assertTrue(query3.isEquivalent(query4));
    }

    @Test
    public void testAlphaEquivalence() {
        String queryString = "match $x isa person;$t isa tag;$t value 'Michelangelo';" +
            "($x, $t) isa tagging;" +
            "$y isa product;$y value 'Michelangelo  The Last Judgement'; select $x, $y;";

        String queryString2 = "match $x isa person;$y isa tag;$y value 'Michelangelo';" +
                "($x, $y) isa tagging;" +
                "$pr isa product;$pr value 'Michelangelo  The Last Judgement'; select $x, $pr;";

        Query query = new Query(queryString, graph);
        Query query2 = new Query(queryString2, graph);

        assertTrue(query.isEquivalent(query2));
    }

    @Test
    public void testQueryResults(){
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(qb.parseMatch("match $x isa person;")));

        answers.forEach(ans -> System.out.println(ans.toString()));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
