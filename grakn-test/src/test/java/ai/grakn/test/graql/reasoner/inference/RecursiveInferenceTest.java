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

package ai.grakn.test.graql.reasoner.inference;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graphs.DiagonalGraph;
import ai.grakn.graphs.MatrixGraph;
import ai.grakn.graphs.MatrixGraphII;
import ai.grakn.graphs.NguyenGraph;
import ai.grakn.graphs.PathGraph;
import ai.grakn.graphs.PathGraphII;
import ai.grakn.graphs.PathGraphSymmetric;
import ai.grakn.graphs.TailRecursionGraph;
import ai.grakn.graphs.TransitivityChainGraph;
import ai.grakn.graphs.TransitivityMatrixGraph;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.reasoner.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;

import ai.grakn.test.GraphContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;


import static ai.grakn.test.GraknTestEnv.usingTinker;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class RecursiveInferenceTest {

    // The recursivity graph is loaded to test if possible, but is unused elsewhere
    @ClassRule
    public static final GraphContext recursivityContext = GraphContext.preLoad("recursivity-test.gql");

    @ClassRule
    public static final GraphContext recursivitySGContext = GraphContext.preLoad("recursivity-sg-test.gql");

    @ClassRule
    public static final GraphContext recursivityTCContext = GraphContext.preLoad("recursivity-tc-test.gql");

    @ClassRule
    public static final GraphContext recursivityRSGContext = GraphContext.preLoad("recursivity-rsg-test.gql");

    @ClassRule
    public static final GraphContext ancestorFriendContext = GraphContext.preLoad("ancestor-friend-test.gql");

    @ClassRule
    public static final GraphContext transitivityContext = GraphContext.preLoad("transitivity-test.gql");

    @ClassRule
    public static final GraphContext ancestorContext = GraphContext.preLoad("ancestor-test.gql");

    @ClassRule
    public static final GraphContext reachabilityContext = GraphContext.preLoad("reachability-test.gql");

    @ClassRule
    public static final GraphContext sameGenerationContext = GraphContext.preLoad("same-generation-test.gql");

    @ClassRule
    public static final GraphContext reachabilitySymmetricContext = GraphContext.preLoad("reachability-test-symmetric.gql");

    @Rule
    public final GraphContext graphContext = GraphContext.empty();

    @Before
    public void onStartup() throws Exception {
        assumeTrue(usingTinker());
        graphContext.graph().close();
    }

    /**from Vieille - Recursive Axioms in Deductive Databases p. 192*/
    @Test
    public void testTransitivity() {
        QueryBuilder qb = transitivityContext.graph().graql().infer(false);
        QueryBuilder iqb = transitivityContext.graph().graql().infer(true);
        String queryString = "match ($x, $y) isa R;$x has index 'i'; select $y;";
        String explicitQuery = "match $y has index $ind;" +
                            "{$ind val 'j';} or {$ind val 's';} or {$ind val 'v';}; select $y;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /*single-directional*/
    /**from Bancilhon - An Amateur's Introduction to Recursive Query Processing Strategies p. 25*/
    @Test
    public void testAncestor() {
        QueryBuilder qb = ancestorContext.graph().graql().infer(false);
        QueryBuilder iqb = ancestorContext.graph().graql().infer(true);

        String queryString = "match (ancestor: $X, descendant: $Y) isa Ancestor;$X has name 'aa';" +
                            "$Y has name $name;select $Y, $name;";
        String explicitQuery = "match $Y isa Person, has name $name;" +
                "{$name val 'aaa';} or {$name val 'aab';} or {$name val 'aaaa';};select $Y, $name;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**as above but both directions*/
    @Test
    public void testAncestorPrime() {
        QueryBuilder qb = ancestorContext.graph().graql().infer(false);
        QueryBuilder iqb = ancestorContext.graph().graql().infer(true);

        String queryString = "match ($X, $Y) isa Ancestor;$X has name 'aa'; select $Y;";
        String explicitQuery = "match $Y isa Person, has name $name;" +
                "{$name val 'a';} or {$name val 'aaa';} or {$name val 'aab';} or {$name val 'aaaa';};select $Y;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testAncestor2() {
        QueryBuilder qb = ancestorContext.graph().graql().infer(false);
        QueryBuilder iqb = ancestorContext.graph().graql().infer(true);

        String queryString = "match (ancestor: $X, descendant: $Y) isa Ancestor;";
        String explicitQuery = "match $Y isa Person, has name $nameY; $X isa Person, has name $nameX;" +
                "{$nameX val 'a';$nameY val 'aa';} or {$nameX val 'a';$nameY val 'ab';} or" +
                "{$nameX val 'a';$nameY val 'aaa';} or {$nameX val 'a';$nameY val 'aab';} or" +
                "{$nameX val 'a';$nameY val 'aaaa';} or {$nameX val 'aa';$nameY val 'aaa';} or" +
                "{$nameX val 'aa';$nameY val 'aab';} or {$nameX val 'aa';$nameY val 'aaaa';} or " +
                "{$nameX val 'aaa';$nameY val 'aaaa';} or {$nameX val 'c';$nameY val 'ca';}; select $X, $Y;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testAncestor2Prime() {
        QueryBuilder qb = ancestorContext.graph().graql().infer(false);
        QueryBuilder iqb = ancestorContext.graph().graql().infer(true);
        String queryString = "match ($X, $Y) isa Ancestor;";
        String explicitQuery = "match $Y isa Person, has name $nameY; $X isa Person, has name $nameX;" +
                "{$nameX val 'a';$nameY val 'aa';} or " +
                "{$nameX val 'a';$nameY val 'ab';} or {$nameX val 'a';$nameY val 'aaa';} or" +
                "{$nameX val 'a';$nameY val 'aab';} or {$nameX val 'a';$nameY val 'aaaa';} or " +
                "{$nameY val 'a';$nameX val 'aa';} or" +
                "{$nameY val 'a';$nameX val 'ab';} or {$nameY val 'a';$nameX val 'aaa';} or" +
                "{$nameY val 'a';$nameX val 'aab';} or {$nameY val 'a';$nameX val 'aaaa';} or "
                +
                "{$nameX val 'aa';$nameY val 'aaa';} or {$nameX val 'aa';$nameY val 'aab';} or" +
                "{$nameX val 'aa';$nameY val 'aaaa';} or " +
                "{$nameY val 'aa';$nameX val 'aaa';} or {$nameY val 'aa';$nameX val 'aab';} or" +
                "{$nameY val 'aa';$nameX val 'aaaa';} or "
                +
                "{$nameX val 'aaa';$nameY val 'aaaa';} or " +
                "{$nameY val 'aaa';$nameX val 'aaaa';} or "
                +
                "{$nameX val 'c';$nameY val 'ca';} or " +
                "{$nameY val 'c';$nameX val 'ca';}; select $X, $Y;";
        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend() {
        QueryBuilder qb = ancestorFriendContext.graph().graql().infer(false);
        QueryBuilder iqb = ancestorFriendContext.graph().graql().infer(true);

        String queryString = "match (person: $X, ancestor-friend: $Y) isa Ancestor-friend;$X has name 'a'; $Y has name $name; select $Y, $name;";
        String explicitQuery = "match $Y has name $name;{$name val 'd';} or {$name val 'g';};";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriendPrime() {
        QueryBuilder qb = ancestorFriendContext.graph().graql().infer(false);
        QueryBuilder iqb = ancestorFriendContext.graph().graql().infer(true);

        String queryString = "match ($X, $Y) isa Ancestor-friend;$X has name 'a'; select $Y;";
        String explicitQuery = "match $Y has name $name;{$name val 'd';} or {$name val 'g';}; select $Y;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend2() {
        QueryBuilder qb = ancestorFriendContext.graph().graql().infer(false);
        QueryBuilder iqb = ancestorFriendContext.graph().graql().infer(true);

        String queryString = "match (person: $X, ancestor-friend: $Y) isa Ancestor-friend;$Y has name 'd'; select $X;";
        String explicitQuery = "match $X has name $name;" +
                "{$name val 'a';} or {$name val 'b';} or {$name val 'c';}; select $X;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend2Prime() {
        QueryBuilder qb = ancestorFriendContext.graph().graql().infer(false);
        QueryBuilder iqb = ancestorFriendContext.graph().graql().infer(true);

        String queryString = "match ($X, $Y) isa Ancestor-friend;$Y has name 'd'; select $X;";
        String explicitQuery = "match $X has name $name;" +
                "{$name val 'a';} or {$name val 'b';} or {$name val 'c';}; select $X;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /*from Vieille - Recursive Query Processing: The power of logic p. 25*/
    /** SG(X, X) :- H(X) doesn't get applied*/
    @Ignore
    @Test
    public void testSameGeneration(){
        QueryBuilder qb = recursivitySGContext.graph().graql().infer(false);
        QueryBuilder iqb = recursivitySGContext.graph().graql().infer(true);

        String queryString = "match ($x, $y) isa SameGen; $x has name 'a'; select $y;";
        String explicitQuery = "match $y has name $name;{$name val 'f';} or {$name val 'h';};select $y;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**from Vieille - Recursive Query Processing: The power of logic p. 18*/
    @Test
    public void testTC() {
        QueryBuilder qb = recursivityTCContext.graph().graql().infer(false);
        QueryBuilder iqb = recursivityTCContext.graph().graql().infer(true);

        String queryString = "match ($x, $y) isa N-TC; $y has index 'a'; select $x;";
        String explicitQuery = "match $x has index 'a2';";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testReachability(){
        QueryBuilder qb = reachabilityContext.graph().graql().infer(false);
        QueryBuilder iqb = reachabilityContext.graph().graql().infer(true);

        String queryString = "match (reach-from: $x, reach-to: $y) isa reachable;";
        String explicitQuery = "match $x has index $indX;$y has index $indY;" +
                "{$indX val 'a';$indY val 'b';} or" +
                "{$indX val 'b';$indY val 'c';} or" +
                "{$indX val 'c';$indY val 'c';} or" +
                "{$indX val 'c';$indY val 'd';} or" +
                "{$indX val 'a';$indY val 'c';} or" +
                "{$indX val 'b';$indY val 'd';} or" +
                "{$indX val 'a';$indY val 'd';};select $x, $y;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    //TODO remodel when repeating roles allowed
    @Test
    public void testReachabilitySymmetric(){
        QueryBuilder qb = reachabilitySymmetricContext.graph().graql().infer(false);
        QueryBuilder iqb = reachabilitySymmetricContext.graph().graql().infer(true);

        String queryString = "match ($x, $y) isa reachable;$x has index 'a';select $y;";
        String explicitQuery = "match $y has index $indY;" +
                "{$indY val 'a';} or {$indY val 'b';} or {$indY val 'c';} or {$indY val 'd';};select $y;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /** test 6.1 from Cao p 71*/
    @Test
    public void testMatrix(){
        final int N = 5;
        graphContext.load(MatrixGraph.get(N, N));
        QueryBuilder qb = graphContext.graph().graql().infer(false);
        QueryBuilder iqb = graphContext.graph().graql().infer(true);

        String queryString = "match (Q1-from: $x, Q1-to: $y) isa Q1; $x has index 'a0'; select $y;";
        String explicitQuery = "match $y isa a-entity or $y isa end;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**
     *  test 6.3 from Cao p 75*/
    @Test
    public void testTailRecursion(){
        final int N = 10;
        final int M = 5;
        graphContext.load(TailRecursionGraph.get(N, M));
        QueryBuilder qb = graphContext.graph().graql().infer(false);
        QueryBuilder iqb = graphContext.graph().graql().infer(true);

        String queryString = "match (P-from: $x, P-to: $y) isa P; $x has index 'a0'; select $y;";
        String explicitQuery = "match $y isa b-entity;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**test3 from Nguyen (similar to test 6.5 from Cao)
     * N(x, y) :- R(x, y)
     * N(x, y) :- P(x, z), N(z, w), Q(w, y)
     *
     *
     *   c -- P -- d -- R -- e -- Q -- a0
     *     \                        /
     *         P               Q
     *      \    \          /
     *                b0   --  Q  --   a1
     *        \                     /
     *          P              Q
     *             \        /
     *                b1   --  Q  --   a2
     *                            .
     *                         .
     *                      .
     *                bN   --  Q --    aN+1
     */
    @Test
    public void testNguyen(){
        final int N = 9;
        graphContext.load(NguyenGraph.get(N));
        QueryBuilder qb = graphContext.graph().graql().infer(false);
        QueryBuilder iqb = graphContext.graph().graql().infer(true);

        String queryString = "match (N-rA: $x, N-rB: $y) isa N; $x has index 'c'; select $y;";
        String explicitQuery = "match $y isa a-entity;";

        QueryAnswers answers = queryAnswers(iqb.materialise(false).parse(queryString));
        QueryAnswers explicitAnswers = queryAnswers(qb.parse(explicitQuery));
        assertEquals(answers.size(), explicitAnswers.size());
        QueryAnswers answers2 = queryAnswers(iqb.materialise(true).parse(queryString));
        assertEquals(answers, answers2);
    }

    /**test 6.6 from Cao p.76*/
    @Test
    public void testSameGenerationCao(){
        QueryBuilder qb = sameGenerationContext.graph().graql().infer(false);
        QueryBuilder iqb = sameGenerationContext.graph().graql().infer(true);

        String queryString = "match ($x, $y) isa SameGen;$x has name 'ann';select $y;";
        String explicitQuery = "match $y has name $name;" +
                "{$name val 'ann';} or {$name val 'bill';} or {$name val 'peter';};select $y;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**test 6.9 from Cao p.82*/
    @Test
    public void testMatrixII(){
        final int N = 5;
        final int M = 5;
        graphContext.load(MatrixGraphII.get(N, M));
        QueryBuilder qb = graphContext.graph().graql().infer(false);
        QueryBuilder iqb = graphContext.graph().graql().infer(true);

        String queryString = "match (P-from: $x, P-to: $y) isa P;$x has index 'a'; select $y;";
        String explicitQuery = "match $y isa a-entity;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**test 6.10 from Cao p. 82*/
    @Test
    public void testPathTree(){
        final int N = 3;
        graphContext.load(PathGraph.get(N, 3));
        GraknGraph graph = graphContext.graph();
        QueryBuilder qb = graph.graql().infer(false);
        QueryBuilder iqb = graph.graql().infer(true);

        Concept a0 = getConcept(graph, "index", "a0");

        String queryString = "match (path-from: $x, path-to: $y) isa path;" +
                "$x has index 'a0';" +
                "select $y;";
        String explicitQuery = "match $y isa vertex;";


        QueryAnswers answers = queryAnswers(iqb.materialise(false).parse(queryString));
        QueryAnswers explicitAnswers = queryAnswers(qb.parse(explicitQuery));
        QueryAnswers answers2 = queryAnswers(iqb.materialise(true).parse(queryString));

        assertEquals(answers.size(), explicitAnswers.size());
        assertEquals(answers, answers2);
    }

    private Concept getConcept(GraknGraph graph, String typeName, Object val){
        return graph.graql().match(Graql.var("x").has(typeName, val).admin()).execute().iterator().next().get("x");
    }

    @Test
    public void testPathTreePrime(){
        final int N = 3;
        graphContext.load(PathGraph.get(N, 3));
        QueryBuilder qb = graphContext.graph().graql().infer(false);
        QueryBuilder iqb = graphContext.graph().graql().infer(true);

        String queryString = "match ($x, $y) isa path;$x has index 'a0'; select $y;";
        String explicitQuery = "match $y isa vertex;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Ignore
    @Test
    public void testPathSymmetric(){
        final int N = 3;
        graphContext.load(PathGraphSymmetric.get(N, 3));
        QueryBuilder qb = graphContext.graph().graql().infer(false);
        QueryBuilder iqb = graphContext.graph().graql().infer(true);

        String queryString = "match ($x, $y) isa path;$x has index 'a0'; select $y;";
        String explicitQuery = "match $y isa vertex;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    /*modified test 6.10 from Cao p. 82*/
    public void testPathII(){
        final int N = 3;
        graphContext.load(PathGraphII.get(N, N));
        QueryBuilder qb = graphContext.graph().graql().infer(false);
        QueryBuilder iqb = graphContext.graph().graql().infer(true);

        String queryString = "match (path-from: $x, path-to: $y) isa path;$x has index 'a0'; select $y;";
        String explicitQuery = "match $y isa vertex;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    /*modified test 6.10 from Cao p. 82*/
    public void testPathIIPrime(){
        final int N = 3;
        graphContext.load(PathGraphII.get(N, N));
        QueryBuilder qb = graphContext.graph().graql().infer(false);
        QueryBuilder iqb = graphContext.graph().graql().infer(true);

        String queryString = "match ($x, $y) isa path;$x has index 'a0'; $y has index $ind;select $y, $ind;";
        String explicitQuery = "match $y isa vertex;$y has index $ind;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    /**from Abiteboul - Foundations of databases p. 312/Cao test 6.14 p. 89*/
    @Test
    public void testReverseSameGeneration(){
        QueryBuilder qb = recursivityRSGContext.graph().graql().infer(false);
        QueryBuilder iqb = recursivityRSGContext.graph().graql().infer(true);

        String queryString = "match (RSG-from: $x, RSG-to: $y) isa RevSG;$x has name 'a'; select $y;";
        String explicitQuery = "match $y isa person, has name $name;" +
                                "{$name val 'b';} or {$name val 'c';} or {$name val 'd';};select $y;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }
    @Test
    public void testReverseSameGeneration2() {
        QueryBuilder qb = recursivityRSGContext.graph().graql().infer(false);
        QueryBuilder iqb = recursivityRSGContext.graph().graql().infer(true);

        String queryString = "match (RSG-from: $x, RSG-to: $y) isa RevSG;";
        String explicitQuery = "match $x has name $nameX;$y has name $nameY;" +
                "{$nameX val 'a';$nameY val 'b';} or {$nameX val 'a';$nameY val 'c';} or" +
                "{$nameX val 'a';$nameY val 'd';} or {$nameX val 'm';$nameY val 'n';} or" +
                "{$nameX val 'm';$nameY val 'o';} or {$nameX val 'p';$nameY val 'm';} or" +
                "{$nameX val 'g';$nameY val 'f';} or {$nameX val 'h';$nameY val 'f';} or" +
                "{$nameX val 'i';$nameY val 'f';} or {$nameX val 'j';$nameY val 'f';} or" +
                "{$nameX val 'f';$nameY val 'k';};select $x, $y;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testTransitiveChain(){
        final int N = 10;
        graphContext.load(TransitivityChainGraph.get(N));
        QueryBuilder qb = graphContext.graph().graql().infer(false);
        QueryBuilder iqb = graphContext.graph().graql().infer(true);

        String queryString = "match (Q-from: $x, Q-to: $y) isa Q;$x has index 'a'; select $y;";
        String explicitQuery = "match $y isa a-entity;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testTransitiveMatrix(){
        final int N = 5;
        graphContext.load(TransitivityMatrixGraph.get(N, N));
        QueryBuilder qb = graphContext.graph().graql().infer(false);
        QueryBuilder iqb = graphContext.graph().graql().infer(true);

        String queryString = "match (Q-from: $x, Q-to: $y) isa Q;$x has index 'a'; select $y;";
        String explicitQuery = "match $y isa a-entity;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testDiagonal(){
        final int N = 10;
        graphContext.load(DiagonalGraph.get(N, N));
        QueryBuilder iqb = graphContext.graph().graql().infer(true);

        String queryString = "match (rel-from: $x, rel-to: $y) isa diagonal;";

        assertEquals(iqb.materialise(false).<MatchQuery>parse(queryString).execute().size(), 64);
        assertEquals(iqb.materialise(true).<MatchQuery>parse(queryString).execute().size(), 64);
    }

    private QueryAnswers queryAnswers(MatchQuery query) {
        return new QueryAnswers(query.admin().streamWithVarNames().map(QueryAnswer::new).collect(toSet()));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        QueryAnswers answers = queryAnswers(q1);
        QueryAnswers answers2 = queryAnswers(q2);
        assertEquals(answers, answers2);
    }
}
