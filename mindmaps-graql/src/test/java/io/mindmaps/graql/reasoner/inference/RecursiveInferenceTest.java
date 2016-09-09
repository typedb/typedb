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
import io.mindmaps.concept.Concept;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryParser;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.graql.reasoner.graphs.*;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class RecursiveInferenceTest {

    /**Misses one expansion of R2 hence not complete result*/
    /**from Vieille - Recursive Axioms in Deductive Databases p. 192*/
    @Test
    public void testTransitivity() {
        MindmapsGraph graph = GenericGraph.getGraph("transitivity-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa R;$x id 'i' select $y";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match $x id 'i';" +
                            "{$y id 'j'} or {$y id 's'} or {$y id 'v'} select $y";

        assertQueriesEqual(reasoner.resolveToQuery(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    @Test
    public void testRecursivity()
    {
        MindmapsGraph graph = GenericGraph.getGraph("recursivity-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);
    }

    /**single-directional*/
    /**from Bancilhon - An Amateur's Introduction to Recursive Query Processing Strategies p. 25*/
    @Test
    public void testAncestor() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (ancestor $X, descendant $Y) isa Ancestor;$X id 'aa' select $Y";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match $Y isa Person;" +
                "{$Y id 'aaa'} or {$Y id 'aab'} or {$Y id 'aaaa'}";

        assertQueriesEqual(reasoner.resolveToQuery(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    /**as above but both directions*/
    @Test
    public void testAncestor2() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($X, $Y) isa Ancestor;$X id 'aa' select $Y";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match $Y isa Person;" +
                "{$Y id 'a'} or {$Y id 'aaa'} or {$Y id 'aab'} or {$Y id 'aaaa'}";

        assertQueriesEqual(reasoner.resolveToQuery(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-friend-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (person $X, ancestor-friend $Y) isa Ancestor-friend;$X id 'a' select $Y";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match $X id 'a';" +
                            "{$Y id 'd'} or {$Y id 'g'} select $Y";

        assertQueriesEqual(reasoner.resolveToQuery(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend2() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-friend-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (person $X, ancestor-friend $Y) isa Ancestor-friend;$Y id 'd' select $X";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match $Y id 'd';" +
                "{$X id 'a'} or {$X id 'b'} or {$X id 'c'} select $X";

        assertQueriesEqual(reasoner.resolveToQuery(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    /**from Vieille - Recursive Query Processing: The power of logic p. 25*/
    /** SG(X, X) :- H(X) doesn't get applied*/
    @Test
    @Ignore
    public void testSameGeneration(){
        MindmapsGraph graph = GenericGraph.getGraph("recursivity-sg-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa SameGen; $x id 'a' select $y";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match {$y id 'f'} or {$y id 'h'};";

        assertQueriesEqual(reasoner.resolveToQuery(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    /**from Vieille - Recursive Query Processing: The power of logic p. 18*/
    @Test
    public void testTC() {
        MindmapsGraph graph = GenericGraph.getGraph("recursivity-tc-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa N-TC; $y id 'a' select $x";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match $x id 'a2';";

        assertQueriesEqual(reasoner.resolveToQuery(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    @Test
    public void testReachability(){
        MindmapsGraph graph = GenericGraph.getGraph("reachability-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (reach-from $x, reach-to $y) isa reachable";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match $x isa vertex;$y isa vertex;" +
                "{$x id 'a';$y id 'b'} or" +
                "{$x id 'b';$y id 'c'} or" +
                "{$x id 'c';$y id 'c'} or" +
                "{$x id 'c';$y id 'd'} or" +
                "{$x id 'a';$y id 'c'} or" +
                "{$x id 'b';$y id 'd'} or" +
                "{$x id 'a';$y id 'd'}";

        assertQueriesEqual(reasoner.resolveToQuery(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    /**test 6.10 from Cao p. 82*/
    @Test
    public void testPath(){
        final int N = 3;
        MindmapsGraph graph = PathGraph.getGraph(N, 3);
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (path-from $x, path-to $y) isa path;$x id 'a0' select $y";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        String explicitQuery = "match $y isa vertex";

        assertQueriesEqual(reasoner.resolveToQuery(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    @Test
    /**modified test 6.10 from Cao p. 82*/
    public void testPathII(){
        final int N = 5;
        MindmapsGraph graph = PathGraphII.getGraph(N, N);
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (path-from $x, path-to $y) isa path;$x id 'a0' select $y";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        String explicitQuery = "match $y isa vertex";

        assertQueriesEqual(reasoner.resolveToQuery(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    /**from Abiteboul - Foundations of databases p. 312/Cao test 6.14 p. 89*/
    @Test
    public void testReverseSameGeneration(){
        MindmapsGraph graph = GenericGraph.getGraph("recursivity-rsg-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (RSG-from $x, RSG-to $y) isa RevSG;$x id 'a' select $y";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        String explicitQuery = "match $y isa person;" +
                                "{$y id 'b'} or {$y id 'c'} or {$y id 'd'}";

        assertQueriesEqual(reasoner.resolveToQuery(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }
    @Test
    public void testReverseSameGeneration2() {
        MindmapsGraph graph = GenericGraph.getGraph("recursivity-rsg-test.gql");
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (RSG-from $x, RSG-to $y) isa RevSG";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        Set<Map<String, Concept>> answers = reasoner.resolve(query);

        String explicitQuery = "match " +
                "{$x id 'a';$y id 'b'} or {$x id 'a';$y id 'c'} or {$x id 'a';$y id 'd'} or" +
                "{$x id 'm';$y id 'n'} or {$x id 'm';$y id 'o'} or {$x id 'p';$y id 'm'} or" +
                "{$x id 'g';$y id 'f'} or {$x id 'h';$y id 'f'} or {$x id 'i';$y id 'f'} or" +
                "{$x id 'j';$y id 'f'} or {$x id 'f';$y id 'k'}";

        assertQueriesEqual(reasoner.resolveToQuery(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertEquals(answers, Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    /**test3 from Nguyen (similar to test 6.5 from Cao)*/
    @Test
    //TODO need to add handling unary predicates to capture match $x isa S
    public void testNguyen(){
        final int N = 9;
        MindmapsGraph graph = NguyenGraph.getGraph(N);
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (N-rA $x, N-rB $y) isa N; $x id 'c' select $y";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        String explicitQuery = "match $y isa a-entity";

        assertQueriesEqual(reasoner.resolveToQuery(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    /** test 6.1 from Cao p 71*/
    @Test
    public void testMatrix(){
        final int N = 5;
        MindmapsGraph graph = MatrixGraph.getGraph(N, N);
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (Q1-from $x, Q1-to $y) isa Q1; $x id 'a0' select $y";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        String explicitQuery = "match $y isa a-entity or $y isa end";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
        assertQueriesEqual(reasoner.resolveToQuery(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());

        queryString = "match (Q2-from $x, Q2-to $y) isa Q2; $x id 'a0' select $y";
        query = qp.parseMatchQuery(queryString).getMatchQuery();
        explicitQuery = "match $y isa b-entity or $y isa end";

        assertQueriesEqual(reasoner.resolveToQuery(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    /** test 6.3 from Cao p 75*/
    @Test
    public void testTailRecursion(){
        final int N = 10;
        final int M = 5;
        MindmapsGraph graph = TailRecursionGraph.getGraph(N, M);
        QueryParser qp = QueryParser.create(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (P-from $x, P-to $y) isa P; $x id 'a0' select $y";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        String explicitQuery = "match $y isa b-entity";

        Set<Map<String, Concept>> answers = reasoner.resolve(query);

        assertQueriesEqual(reasoner.resolveToQuery(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertEquals(answers, Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
