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

package io.mindmaps.test.graql.reasoner.inference;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.test.graql.reasoner.graphs.GenericGraph;
import io.mindmaps.test.graql.reasoner.graphs.MatrixGraph;
import io.mindmaps.test.graql.reasoner.graphs.MatrixGraphII;
import io.mindmaps.test.graql.reasoner.graphs.NguyenGraph;
import io.mindmaps.test.graql.reasoner.graphs.PathGraph;
import io.mindmaps.test.graql.reasoner.graphs.PathGraphII;
import io.mindmaps.test.graql.reasoner.graphs.PathGraphSymmetric;
import io.mindmaps.test.graql.reasoner.graphs.TailRecursionGraph;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static io.mindmaps.graql.internal.reasoner.Utility.printAnswers;
import static org.junit.Assert.assertEquals;

public class RecursiveInferenceTest {

    /**from Vieille - Recursive Axioms in Deductive Databases p. 192*/
    @Test
    public void testTransitivity() {
        MindmapsGraph graph = GenericGraph.getGraph("transitivity-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa R;$x id 'i'; select $y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $x id 'i';" +
                            "{$y id 'j';} or {$y id 's';} or {$y id 'v';}; select $y;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertEquals(Sets.newHashSet(qb.<MatchQuery>parse(queryString)), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    public void testRecursivity()
    {
        MindmapsGraph graph = GenericGraph.getGraph("recursivity-test.gql");
        Reasoner reasoner = new Reasoner(graph);
    }

    /**single-directional*/
    /**from Bancilhon - An Amateur's Introduction to Recursive Query Processing Strategies p. 25*/
    @Test
    public void testAncestor() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (ancestor: $X, descendant: $Y) isa Ancestor;$X id 'aa'; select $Y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $Y isa Person;" +
                "{$Y id 'aaa';} or {$Y id 'aab';} or {$Y id 'aaaa';};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**as above but both directions*/
    @Test
    public void testAncestorPrime() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($X, $Y) isa Ancestor;$X id 'aa'; select $Y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $Y isa Person;" +
                "{$Y id 'a';} or {$Y id 'aaa';} or {$Y id 'aab';} or {$Y id 'aaaa';};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    public void testAncestor2() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (ancestor: $X, descendant: $Y) isa Ancestor;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $Y isa Person;" +
                "{$X id 'a';$Y id 'aa';} or {$X id 'a';$Y id 'ab';} or {$X id 'a';$Y id 'aaa';} or {$X id 'a';$Y id 'aab';} or {$X id 'a';$Y id 'aaaa';} or " +
                "{$X id 'aa';$Y id 'aaa';} or {$X id 'aa';$Y id 'aab';} or {$X id 'aa';$Y id 'aaaa';} or " +
                "{$X id 'aaa';$Y id 'aaaa';} or " +
                "{$X id 'c';$Y id 'ca';};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    public void testAncestor2Prime() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($X, $Y) isa Ancestor;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $Y isa Person;" +
                "{$X id 'a';$Y id 'aa';} or {$X id 'a';$Y id 'ab';} or {$X id 'a';$Y id 'aaa';} or {$X id 'a';$Y id 'aab';} or {$X id 'a';$Y id 'aaaa';} or " +
                "{$Y id 'a';$X id 'aa';} or {$Y id 'a';$X id 'ab';} or {$Y id 'a';$X id 'aaa';} or {$Y id 'a';$X id 'aab';} or {$Y id 'a';$X id 'aaaa';} or " +
                "{$X id 'aa';$Y id 'aaa';} or {$X id 'aa';$Y id 'aab';} or {$X id 'aa';$Y id 'aaaa';} or " +
                "{$Y id 'aa';$X id 'aaa';} or {$Y id 'aa';$X id 'aab';} or {$Y id 'aa';$X id 'aaaa';} or " +
                "{$X id 'aaa';$Y id 'aaaa';} or " +
                "{$Y id 'aaa';$X id 'aaaa';} or " +
                "{$X id 'c';$Y id 'ca';} or " +
                "{$Y id 'c';$X id 'ca';};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-friend-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (person: $X, ancestor-friend: $Y) isa Ancestor-friend;$X id 'a'; select $Y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $X id 'a';{$Y id 'd';} or {$Y id 'g';}; select $Y;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        reasoner.resolve(query, true);
        assertEquals(Sets.newHashSet(qb.<MatchQuery>parse(queryString)), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriendPrime() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-friend-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($X, $Y) isa Ancestor-friend;$X id 'a'; select $Y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $X id 'a';{$Y id 'd';} or {$Y id 'g';}; select $Y;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend2() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-friend-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (person: $X, ancestor-friend: $Y) isa Ancestor-friend;$Y id 'd'; select $X;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $Y id 'd';" +
                "{$X id 'a';} or {$X id 'b';} or {$X id 'c';}; select $X;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend2Prime() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-friend-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($X, $Y) isa Ancestor-friend;$Y id 'd'; select $X;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $Y id 'd';" +
                                "{$X id 'a';} or {$X id 'b';} or {$X id 'c';}; select $X;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**from Vieille - Recursive Query Processing: The power of logic p. 25*/
    /** SG(X, X) :- H(X) doesn't get applied*/
    @Test
    @Ignore
    public void testSameGeneration(){
        MindmapsGraph graph = GenericGraph.getGraph("recursivity-sg-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa SameGen; $x id 'a' select $y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match {$y id 'f'} or {$y id 'h'};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**from Vieille - Recursive Query Processing: The power of logic p. 18*/
    @Test
    public void testTC() {
        MindmapsGraph graph = GenericGraph.getGraph("recursivity-tc-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa N-TC; $y id 'a'; select $x;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $x id 'a2';";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    public void testReachability(){
        MindmapsGraph graph = GenericGraph.getGraph("reachability-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (reach-from: $x, reach-to: $y) isa reachable;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $x isa vertex;$y isa vertex;" +
                "{$x id 'a';$y id 'b';} or" +
                "{$x id 'b';$y id 'c';} or" +
                "{$x id 'c';$y id 'c';} or" +
                "{$x id 'c';$y id 'd';} or" +
                "{$x id 'a';$y id 'c';} or" +
                "{$x id 'b';$y id 'd';} or" +
                "{$x id 'a';$y id 'd';};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    public void testReachabilitySymmetric(){
        MindmapsGraph graph = GenericGraph.getGraph("reachability-test-symmetric.gql");
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa reachable;$x id 'a';";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $y isa vertex;" +
                "{$y id 'a';} or {$y id 'b';} or {$y id 'c';} or {$y id 'd';};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /** test 6.1 from Cao p 71*/
    @Test
    public void testMatrix(){
        final int N = 5;
        MindmapsGraph graph = MatrixGraph.getGraph(N, N);
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (Q1-from: $x, Q1-to: $y) isa Q1; $x id 'a0'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa a-entity or $y isa end;";

        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /** test 6.3 from Cao p 75*/
    @Test
    public void testTailRecursion(){
        final int N = 10;
        final int M = 5;
        MindmapsGraph graph = TailRecursionGraph.getGraph(N, M);
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (P-from: $x, P-to: $y) isa P; $x id 'a0'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa b-entity;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**test3 from Nguyen (similar to test 6.5 from Cao)*/
    @Test
    public void testNguyen(){
        final int N = 9;
        MindmapsGraph graph = NguyenGraph.getGraph(N);
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (N-rA: $x, N-rB: $y) isa N; $x id 'c'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa a-entity;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    public void testNguyen2(){
        final int N = 9;
        MindmapsGraph graph = NguyenGraph.getGraph(N);
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match $y isa S;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa a-entity;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**test 6.6 from Cao p.76*/
    @Test
    public void testSameGenerationCao(){
        MindmapsGraph graph = GenericGraph.getGraph("same-generation-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa SameGen;$x id 'ann';";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match {$y id 'ann';} or {$y id 'bill';} or {$y id 'peter';};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**test 6.9 from Cao p.82*/
    @Test
    public void testMatrixII(){
        final int N = 5;
        final int M = 5;
        MindmapsGraph graph = MatrixGraphII.getGraph(N, M);
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (P-from: $x, P-to: $y) isa P;$x id 'a'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa a-entity;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**test 6.10 from Cao p. 82*/
    @Test
    public void testPath(){
        final int N = 3;
        MindmapsGraph graph = PathGraph.getGraph(N, 3);
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (path-from: $x, path-to: $y) isa path;$x id 'a0'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa vertex;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    public void testPathPrime(){
        final int N = 3;
        MindmapsGraph graph = PathGraph.getGraph(N, 3);
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa path;$x id 'a0'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa vertex;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    @Ignore
    public void testPathSymmetric(){
        final int N = 3;
        MindmapsGraph graph = PathGraphSymmetric.getGraph(N, 3);
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa path;$x id 'a0'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa vertex;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    /**modified test 6.10 from Cao p. 82*/
    public void testPathII(){
        final int N = 3;
        MindmapsGraph graph = PathGraphII.getGraph(N, N);
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (path-from: $x, path-to: $y) isa path;$x id 'a0'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa vertex;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    /**modified test 6.10 from Cao p. 82*/
    public void testPathIIPrime(){
        final int N = 3;
        MindmapsGraph graph = PathGraphII.getGraph(N, N);
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa path;$x id 'a0'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa vertex;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }
    
    /**from Abiteboul - Foundations of databases p. 312/Cao test 6.14 p. 89*/
    @Test
    public void testReverseSameGeneration(){
        MindmapsGraph graph = GenericGraph.getGraph("recursivity-rsg-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (RSG-from: $x, RSG-to: $y) isa RevSG;$x id 'a'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa person;" +
                                "{$y id 'b';} or {$y id 'c';} or {$y id 'd';};";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }
    @Test
    public void testReverseSameGeneration2() {
        MindmapsGraph graph = GenericGraph.getGraph("recursivity-rsg-test.gql");
        QueryBuilder qb = Graql.withGraph(graph);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (RSG-from: $x, RSG-to: $y) isa RevSG;";
        MatchQuery query = qb.parse(queryString);
        Set<Map<String, Concept>> answers = reasoner.resolve(query);

        String explicitQuery = "match " +
                "{$x id 'a';$y id 'b';} or {$x id 'a';$y id 'c';} or {$x id 'a';$y id 'd';} or" +
                "{$x id 'm';$y id 'n';} or {$x id 'm';$y id 'o';} or {$x id 'p';$y id 'm';} or" +
                "{$x id 'g';$y id 'f';} or {$x id 'h';$y id 'f';} or {$x id 'i';$y id 'f';} or" +
                "{$x id 'j';$y id 'f';} or {$x id 'f';$y id 'k';};";

        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
