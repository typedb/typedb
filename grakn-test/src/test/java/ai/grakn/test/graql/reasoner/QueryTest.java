/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
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

package ai.grakn.test.graql.reasoner;

import ai.grakn.GraknGraph;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.reasoner.query.AtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.test.AbstractGraknTest;
import ai.grakn.test.graql.reasoner.graphs.GeoGraph;
import ai.grakn.test.graql.reasoner.graphs.SNBGraph;
import ai.grakn.test.graql.reasoner.graphs.TestGraph;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class QueryTest extends AbstractGraknTest {

    private static GraknGraph graph;

    @BeforeClass
    public static void setUpClass() throws Exception {
        assumeTrue(usingTinker());
        graph = SNBGraph.getGraph();
    }

    @Test
    public void testCopyConstructor(){
        String queryString = "match $x isa person;$y isa product;($x, $y) isa recommendation;";
        ReasonerQueryImpl query = new ReasonerQueryImpl(queryString, graph);
        ReasonerQueryImpl copy = new ReasonerQueryImpl(query);

        assertQueriesEqual(query.getMatchQuery(), copy.getMatchQuery());
    }

    @Test
    public void testTwinPattern() {
        String queryString2 = "match $x isa person, has name 'Bob';";
        String queryString = "match $x isa person;$x has name 'Bob';";
        String queryString3 = "match $x isa person, value 'Bob';";
        String queryString4 = "match $x isa person;$x value 'Bob';";

        ReasonerQueryImpl query = new ReasonerQueryImpl(queryString, graph);
        ReasonerQueryImpl query2 = new ReasonerQueryImpl(queryString2, graph);
        ReasonerQueryImpl query3 = new ReasonerQueryImpl(queryString3, graph);
        ReasonerQueryImpl query4 = new ReasonerQueryImpl(queryString4, graph);

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

        ReasonerQueryImpl query = new ReasonerQueryImpl(queryString, graph);
        ReasonerQueryImpl query2 = new ReasonerQueryImpl(queryString2, graph);

        assertTrue(query.isEquivalent(query2));
    }

    @Test
    public void testAlphaEquivalence2() {
        GraknGraph lgraph = TestGraph.getGraph("name", "ancestor-friend-test.gql");

        String queryString = "match $X id 'a'; (ancestor-friend: $X, person: $Y), isa Ancestor-friend; select $Y;";
        String queryString2 = "match $X id 'a'; (person: $X, ancestor-friend: $Y), isa Ancestor-friend; select $Y;";

        ReasonerQueryImpl query = new ReasonerQueryImpl(queryString, lgraph);
        ReasonerQueryImpl query2 = new ReasonerQueryImpl(queryString2, lgraph);

        assertTrue(!query.isEquivalent(query2));
    }

    @Test
    public void testAlphaEquivalence3() {
        GraknGraph lgraph = GeoGraph.getGraph();

        String queryString = "match $y id 'Poland'; $y isa country; (geo-entity: $y1, entity-location: $y), isa is-located-in; select $y1;";
        String queryString2 = "match $y id 'Poland'; $x isa city; (geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country; select $x;";
        String queryString3 = "match $x isa city; (entity-location: $y1, geo-entity: $x), isa is-located-in; select $y1, $x;";
        String queryString4 = "match (geo-entity: $y1, entity-location: $y2), isa is-located-in; select $y1, $y2;";

        ReasonerQueryImpl query = new ReasonerQueryImpl(queryString, lgraph);
        ReasonerQueryImpl query2 = new ReasonerQueryImpl(queryString2, lgraph);
        ReasonerQueryImpl query3 = new ReasonerQueryImpl(queryString3, lgraph);
        ReasonerQueryImpl query4 = new ReasonerQueryImpl(queryString4, lgraph);

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

        ReasonerQueryImpl query5 = new ReasonerQueryImpl(queryString5, lgraph);
        ReasonerQueryImpl query6 = new ReasonerQueryImpl(queryString6, lgraph);
        ReasonerQueryImpl query7 = new ReasonerQueryImpl(queryString7, lgraph);
        ReasonerQueryImpl query8 = new ReasonerQueryImpl(queryString8, lgraph);

        assertTrue(query5.isEquivalent(query6));
        assertTrue(query7.isEquivalent(query8));
    }

    @Test
    public void testUnification(){
        GraknGraph lgraph = GeoGraph.getGraph();
        String parentQueryString = "match (entity-location: $y, geo-entity: $y1), isa is-located-in; select $y1, $y;";
        String childQueryString = "match (geo-entity: $y1, entity-location: $y2), isa is-located-in; select $y1, $y2;";

        AtomicQuery parentQuery = new AtomicQuery(parentQueryString, lgraph);
        AtomicQuery childQuery = new AtomicQuery(childQueryString, lgraph);

        Atomic childAtom = childQuery.getAtom();
        Atomic parentAtom = parentQuery.getAtom();

        Map<VarName, VarName> unifiers = childAtom.getUnifiers(parentAtom);

        AtomicQuery childCopy = new AtomicQuery(childQuery.toString(), lgraph);
        childCopy.unify(unifiers);
        Atomic childAtomCopy = childCopy.getAtom();

        assertTrue(!childAtomCopy.equals(childAtom));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
