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
import io.mindmaps.graql.Reasoner;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;
import io.mindmaps.graql.internal.reasoner.predicate.AtomicFactory;
import io.mindmaps.graql.internal.reasoner.query.AtomicQuery;
import io.mindmaps.graql.internal.reasoner.query.Query;
import io.mindmaps.graql.internal.reasoner.query.QueryAnswers;
import io.mindmaps.graql.reasoner.graphs.GenericGraph;
import io.mindmaps.graql.reasoner.graphs.GeoGraph;
import io.mindmaps.graql.reasoner.graphs.SNBGraph;
import java.util.Map;
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
        String queryString = "match $x isa person;$x value 'Bob';$x id 'Bob';";

        Query query = new Query(queryString, graph);
        Atomic vpAtom = AtomicFactory
                .create(qb.parseMatch("match $x value 'Bob';").admin().getPattern().getPatterns().iterator().next());
        Atomic subAtom = AtomicFactory
                .create(qb.parseMatch("match $x id 'Bob';").admin().getPattern().getPatterns().iterator().next());
        assertTrue(query.containsAtom(vpAtom));
        assertTrue(query.containsAtom(subAtom));
        assertEquals(query.getValuePredicate("x"), "Bob");
        assertEquals(query.getSubstitution("x"), "Bob");
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
        String queryString2 = "match $x isa person, id 'Bob';";
        String queryString = "match $x isa person;$x id 'Bob';";
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
    public void testAlphaEquivalence2() {
        MindmapsGraph lgraph = GenericGraph.getGraph("ancestor-friend-test.gql");

        String queryString = "match $X id 'a'; (ancestor-friend: $X, person: $Y), isa Ancestor-friend; select $Y;";
        String queryString2 = "match $X id 'a'; (person: $X, ancestor-friend: $Y), isa Ancestor-friend; select $Y;";

        Query query = new Query(queryString, lgraph);
        Query query2 = new Query(queryString2, lgraph);

        assertTrue(!query.isEquivalent(query2));
    }

    @Test
    public void testAlphaEquivalence3() {
        MindmapsGraph lgraph = GeoGraph.getGraph();

        String queryString = "match $y id 'Poland'; $y isa country; (geo-entity: $y1, entity-location: $y), isa is-located-in; select $y1;";
        String queryString2 = "match $y id 'Poland'; $x isa city; (geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country; select $x;";
        String queryString3 = "match $x isa city; (entity-location: $y1, geo-entity: $x), isa is-located-in; select $y1, $x;";
        String queryString4 = "match (geo-entity: $y1, entity-location: $y2), isa is-located-in; select $y1, $y2;";

        Query query = new Query(queryString, lgraph);
        Query query2 = new Query(queryString2, lgraph);
        Query query3 = new Query(queryString3, lgraph);
        Query query4 = new Query(queryString4, lgraph);

        assertTrue(!query.isEquivalent(query2));
        assertTrue(!query.isEquivalent(query3));
        assertTrue(!query.isEquivalent(query4));

        assertTrue(!query2.isEquivalent(query3));
        assertTrue(!query2.isEquivalent(query4));

        assertTrue(!query3.isEquivalent(query4));

        String queryString5 = "match (entity-location: $y, geo-entity: $y1), isa is-located-in; select $y1, $y2;";
        String queryString6 = "match (geo-entity: $y1, entity-location: $y2), isa is-located-in; select $y1, $y2;";
        String queryString7 = "match (entity-location: $y, geo-entity: $x), isa is-located-in; $x isa city; select $y1, $x;";
        String queryString8 = "match $x isa city; (entity-location: $y1, geo-entity: $x), isa is-located-in; select $y1, $x;";

        Query query5 = new Query(queryString5, lgraph);
        Query query6 = new Query(queryString6, lgraph);
        Query query7 = new Query(queryString7, lgraph);
        Query query8 = new Query(queryString8, lgraph);

        assertTrue(query5.isEquivalent(query6));
        assertTrue(query7.isEquivalent(query8));
    }

    @Test
    public void testUnification(){
        MindmapsGraph lgraph = GeoGraph.getGraph();
        String parentQueryString = "match (entity-location: $y, geo-entity: $y1), isa is-located-in; select $y1, $y;";
        String childQueryString = "match (geo-entity: $y1, entity-location: $y2), isa is-located-in; select $y1, $y2;";

        AtomicQuery parentQuery = new AtomicQuery(parentQueryString, lgraph);
        AtomicQuery childQuery = new AtomicQuery(childQueryString, lgraph);

        Atomic childAtom = childQuery.getAtom();
        Atomic parentAtom = parentQuery.getAtom();

        Map<String, String> unifiers = childAtom.getUnifiers(parentAtom);

        AtomicQuery childCopy = new AtomicQuery(childQuery.toString(), graph);
        childCopy.unify(unifiers);
        Atomic childAtomCopy = childCopy.getAtom();

        assertTrue(!childAtomCopy.equals(childAtom));
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
