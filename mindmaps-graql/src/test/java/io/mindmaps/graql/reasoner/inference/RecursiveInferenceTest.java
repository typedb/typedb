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
import io.mindmaps.graql.reasoner.graphs.GenericGraph;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RecursiveInferenceTest {

    /**Misses one expansion of R2 hence not complete result*/
    /**from Vieille - Recursive Axioms in Deductive Databases p. 192*/
    @Test
    public void testTransitivity() {
        MindmapsTransaction graph = GenericGraph.getTransaction("transitivity-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa R;$x id 'i' select $y";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match $x id 'i';" +
                            "{$y id 'j'} or {$y id 's'} or {$y id 'v'} select $y";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    @Test
    public void testRecursivity()
    {
        MindmapsTransaction graph = GenericGraph.getTransaction("recursivity-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);
    }

    /**single-directional*/
    /**from Bancilhon - An Amateur's Introduction to Recursive Query Processing Strategies p. 25*/
    @Test
    public void testAncestor() {
        MindmapsTransaction graph = GenericGraph.getTransaction("ancestor-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (ancestor $X, descendant $Y) isa Ancestor;$X id 'aa' select $Y";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match $Y isa Person;" +
                "{$Y id 'aaa'} or {$Y id 'aab'} or {$Y id 'aaaa'}";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    /**as above but both directions*/
    //produces surplus relation due to role confusion
    @Test
    @Ignore
    public void testAncestor2() {
        MindmapsTransaction graph = GenericGraph.getTransaction("ancestor-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($X, $Y) isa Ancestor;$X id 'aa' select $Y";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match $Y isa Person;" +
                "{$Y id 'a'} or {$Y id 'aaa'} or {$Y id 'aab'} or {$Y id 'aaaa'}";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend() {
        MindmapsTransaction graph = GenericGraph.getTransaction("ancestor-friend-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (person $X, ancestor-friend $Y) isa Ancestor-friend;$X id 'a' select $Y";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match $X id 'a';" +
                            "{$Y id 'd'} or {$Y id 'g'} select $Y";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend2() {
        MindmapsTransaction graph = GenericGraph.getTransaction("ancestor-friend-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (person $X, ancestor-friend $Y) isa Ancestor-friend;$Y id 'd' select $X";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match $Y id 'd';" +
                "{$X id 'a'} or {$X id 'b'} or {$X id 'c'} select $X";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    /**from Vieille - Recursive Query Processing: The power of logic p. 25*/
    /** SG(X, X) :- H(X) doesn't get applied*/
    @Test
    @Ignore
    public void testSameGeneration(){
        MindmapsTransaction graph = GenericGraph.getTransaction("recursivity-sg-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa SameGen; $x id 'a' select $y";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match {$y id 'f'} or {$y id 'h'};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    /**from Vieille - Recursive Query Processing: The power of logic p. 18*/
    @Test
    public void testTC() {
        MindmapsTransaction graph = GenericGraph.getTransaction("recursivity-tc-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa N-TC; $y id 'a' select $x";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match $x id 'a2';";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    private void assertQueriesEqual(MatchQueryDefault q1, MatchQueryDefault q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
