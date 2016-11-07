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
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilderImpl;
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
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa R;$x has index 'i'; select $y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $y has index $ind;" +
                            "{$ind value 'j';} or {$ind value 's';} or {$ind value 'v';}; select $y;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        //assertEquals(Sets.newHashSet(qb.<MatchQuery>parse(queryString)), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
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
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (ancestor: $X, descendant: $Y) isa Ancestor;$X has name 'aa';" +
                            "$Y has name $name;select $Y, $name;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $Y isa Person, has name $name;" +
                "{$name value 'aaa';} or {$name value 'aab';} or {$name value 'aaaa';};select $Y, $name;";

        printAnswers(reasoner.resolve(query));
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**as above but both directions*/
    @Test
    public void testAncestorPrime() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-test.gql");
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($X, $Y) isa Ancestor;$X has name 'aa'; select $Y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $Y isa Person, has name $name;" +
                "{$name value 'a';} or {$name value 'aaa';} or {$name value 'aab';} or {$name value 'aaaa';};select $Y;";

        printAnswers(reasoner.resolve(query));
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    public void testAncestor2() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-test.gql");
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (ancestor: $X, descendant: $Y) isa Ancestor;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $Y isa Person, has name $nameY; $X isa Person, has name $nameX;" +
                "{$nameX value 'a';$nameY value 'aa';} or {$nameX value 'a';$nameY value 'ab';} or" +
                "{$nameX value 'a';$nameY value 'aaa';} or {$nameX value 'a';$nameY value 'aab';} or" +
                "{$nameX value 'a';$nameY value 'aaaa';} or {$nameX value 'aa';$nameY value 'aaa';} or" +
                "{$nameX value 'aa';$nameY value 'aab';} or {$nameX value 'aa';$nameY value 'aaaa';} or " +
                "{$nameX value 'aaa';$nameY value 'aaaa';} or {$nameX value 'c';$nameY value 'ca';}; select $X, $Y;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    public void testAncestor2Prime() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-test.gql");
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($X, $Y) isa Ancestor;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $Y isa Person, has name $nameY; $X isa Person, has name $nameX;" +
                "{$nameX value 'a';$nameY value 'aa';} or {$nameX value 'a';$nameY value 'ab';} or" +
                "{$nameX value 'a';$nameY value 'aaa';} or {$nameX value 'a';$nameY value 'aab';} or" +
                "{$nameX value 'a';$nameY value 'aaaa';} or {$nameY value 'a';$nameX value 'aa';} or" +
                "{$nameY value 'a';$nameX value 'ab';} or {$nameY value 'a';$nameX value 'aaa';} or" +
                "{$nameY value 'a';$nameX value 'aab';} or {$nameY value 'a';$nameX value 'aaaa';} or " +
                "{$nameX value 'aa';$nameY value 'aaa';} or {$nameX value 'aa';$nameY value 'aab';} or" +
                "{$nameX value 'aa';$nameY value 'aaaa';} or {$nameY value 'aa';$nameX value 'aaa';} or" +
                "{$nameY value 'aa';$nameX value 'aab';} or {$nameY value 'aa';$nameX value 'aaaa';} or " +
                "{$nameX value 'aaa';$nameY value 'aaaa';} or " +
                "{$nameY value 'aaa';$nameX value 'aaaa';} or " +
                "{$nameX value 'c';$nameY value 'ca';} or " +
                "{$nameY value 'c';$nameX value 'ca';}; select $X, $Y;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-friend-test.gql");
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (person: $X, ancestor-friend: $Y) isa Ancestor-friend;$X has name 'a'; select $Y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $Y has name $name;{$name value 'd';} or {$name value 'g';}; select $Y;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //reasoner.resolve(query, true);
        //assertEquals(Sets.newHashSet(qb.<MatchQuery>parse(queryString)), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriendPrime() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-friend-test.gql");
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($X, $Y) isa Ancestor-friend;$X has name 'a'; select $Y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $Y has name $name;{$name value 'd';} or {$name value 'g';}; select $Y;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend2() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-friend-test.gql");
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (person: $X, ancestor-friend: $Y) isa Ancestor-friend;$Y has name 'd'; select $X;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $X has name $name;" +
                "{$name value 'a';} or {$name value 'b';} or {$name value 'c';}; select $X;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend2Prime() {
        MindmapsGraph graph = GenericGraph.getGraph("ancestor-friend-test.gql");
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($X, $Y) isa Ancestor-friend;$Y has name 'd'; select $X;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $X has name $name;" +
                "{$name value 'a';} or {$name value 'b';} or {$name value 'c';}; select $X;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**from Vieille - Recursive Query Processing: The power of logic p. 25*/
    /** SG(X, X) :- H(X) doesn't get applied*/
    @Test
    @Ignore
    public void testSameGeneration(){
        MindmapsGraph graph = GenericGraph.getGraph("recursivity-sg-test.gql");
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa SameGen; $x has name 'a'; select $y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $y has name $name;{$name value 'f';} or {$name value 'h';};select $y;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**from Vieille - Recursive Query Processing: The power of logic p. 18*/
    @Test
    public void testTC() {
        MindmapsGraph graph = GenericGraph.getGraph("recursivity-tc-test.gql");
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa N-TC; $y has index 'a'; select $x;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $x has index 'a2';";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    public void testReachability(){
        MindmapsGraph graph = GenericGraph.getGraph("reachability-test.gql");
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (reach-from: $x, reach-to: $y) isa reachable;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $x has index $indX;$y has index $indY;" +
                "{$indX value 'a';$indY value 'b';} or" +
                "{$indX value 'b';$indY value 'c';} or" +
                "{$indX value 'c';$indY value 'c';} or" +
                "{$indX value 'c';$indY value 'd';} or" +
                "{$indX value 'a';$indY value 'c';} or" +
                "{$indX value 'b';$indY value 'd';} or" +
                "{$indX value 'a';$indY value 'd';};select $x, $y;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    public void testReachabilitySymmetric(){
        MindmapsGraph graph = GenericGraph.getGraph("reachability-test-symmetric.gql");
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa reachable;$x has index 'a';select $y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $y has index $indY;" +
                "{$indY value 'a';} or {$indY value 'b';} or {$indY value 'c';} or {$indY value 'd';};select $y;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /** test 6.1 from Cao p 71*/
    @Test
    public void testMatrix(){
        final int N = 5;
        MindmapsGraph graph = MatrixGraph.getGraph(N, N);
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (Q1-from: $x, Q1-to: $y) isa Q1; $x has index 'a0'; select $y;";
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
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (P-from: $x, P-to: $y) isa P; $x has index 'a0'; select $y;";
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
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (N-rA: $x, N-rB: $y) isa N; $x has index 'c'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa a-entity;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    //TODO bug #10635
    @Test
    @Ignore
    public void testNguyen2(){
        final int N = 9;
        MindmapsGraph graph = NguyenGraph.getGraph(N);
        QueryBuilderImpl qb = graph.graql();
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
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa SameGen;$x has name 'ann';select $y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $y has name $name;" +
                "{$name value 'ann';} or {$name value 'bill';} or {$name value 'peter';};select $y;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**test 6.9 from Cao p.82*/
    @Test
    public void testMatrixII(){
        final int N = 5;
        final int M = 5;
        MindmapsGraph graph = MatrixGraphII.getGraph(N, M);
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (P-from: $x, P-to: $y) isa P;$x has index 'a'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa a-entity;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**test 6.10 from Cao p. 82*/
    @Test
    public void testPath(){
        final int N = 3;
        MindmapsGraph graph = PathGraph.getGraph(N, 3);
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (path-from: $x, path-to: $y) isa path;$x has index 'a0'; select $y;";
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
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa path;$x has index 'a0'; select $y;";
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
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa path;$x has index 'a0'; select $y;";
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
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (path-from: $x, path-to: $y) isa path;$x has index 'a0'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa vertex;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    @Test
    /**modified test 6.10 from Cao p. 82*/
    public void testPathIIPrime(){
        final int N = 3;
        MindmapsGraph graph = PathGraphII.getGraph(N, N);
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match ($x, $y) isa path;$x has index 'a0'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa vertex;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
       // assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    /**from Abiteboul - Foundations of databases p. 312/Cao test 6.14 p. 89*/
    @Test
    public void testReverseSameGeneration(){
        MindmapsGraph graph = GenericGraph.getGraph("recursivity-rsg-test.gql");
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (RSG-from: $x, RSG-to: $y) isa RevSG;$x has name 'a'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa person, has name $name;" +
                                "{$name value 'b';} or {$name value 'c';} or {$name value 'd';};select $y;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }
    @Test
    public void testReverseSameGeneration2() {
        MindmapsGraph graph = GenericGraph.getGraph("recursivity-rsg-test.gql");
        QueryBuilderImpl qb = graph.graql();
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match (RSG-from: $x, RSG-to: $y) isa RevSG;";
        MatchQuery query = qb.parse(queryString);
        Set<Map<String, Concept>> answers = reasoner.resolve(query);

        String explicitQuery = "match $x has name $nameX;$y has name $nameY;" +
                "{$nameX value 'a';$nameY value 'b';} or {$nameX value 'a';$nameY value 'c';} or" +
                "{$nameX value 'a';$nameY value 'd';} or {$nameX value 'm';$nameY value 'n';} or" +
                "{$nameX value 'm';$nameY value 'o';} or {$nameX value 'p';$nameY value 'm';} or" +
                "{$nameX value 'g';$nameY value 'f';} or {$nameX value 'h';$nameY value 'f';} or" +
                "{$nameX value 'i';$nameY value 'f';} or {$nameX value 'j';$nameY value 'f';} or" +
                "{$nameX value 'f';$nameY value 'k';};select $x, $y;";

        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
        //assertEquals(reasoner.resolve(query, true), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
