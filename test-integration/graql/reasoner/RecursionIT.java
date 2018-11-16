/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.reasoner;

import grakn.core.graql.GetQuery;
import grakn.core.graql.Graql;
import grakn.core.graql.Query;
import grakn.core.graql.QueryBuilder;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.reasoner.graph.DualLinearTransitivityMatrixGraph;
import grakn.core.graql.reasoner.graph.LinearTransitivityMatrixGraph;
import grakn.core.graql.reasoner.graph.NguyenGraph;
import grakn.core.graql.reasoner.graph.PathMatrixGraph;
import grakn.core.graql.reasoner.graph.PathTreeGraph;
import grakn.core.graql.reasoner.graph.PathTreeSymmetricGraph;
import grakn.core.graql.reasoner.graph.TailRecursionGraph;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionImpl;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class RecursionIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl recursivitySGSession;
    private static SessionImpl recursivityTCSession;
    private static SessionImpl recursivityRSGSession;

    private static SessionImpl ancestorFriendSession;
    private static SessionImpl ancestorSession;
    private static SessionImpl transitivitySession;

    private static SessionImpl reachabilitySession;
    private static SessionImpl reachabilitySymmetricSession;
    private static SessionImpl sameGenerationSession;

    private static SessionImpl pathTreeSession;
    private static SessionImpl pathMatrixSession;


    @BeforeClass
    public static void loadSession() {
        recursivitySGSession = server.sessionWithNewKeyspace();
        loadFromFile("recursivity-sg.gql", recursivitySGSession);
        recursivityTCSession = server.sessionWithNewKeyspace();
        loadFromFile("recursivity-tc.gql", recursivityTCSession);
        recursivityRSGSession = server.sessionWithNewKeyspace();
        loadFromFile("recursivity-rsg.gql", recursivityRSGSession);

        ancestorFriendSession = server.sessionWithNewKeyspace();
        loadFromFile("ancestor-friend.gql", ancestorFriendSession);
        transitivitySession = server.sessionWithNewKeyspace();
        loadFromFile("transitivity.gql", transitivitySession);
        ancestorSession = server.sessionWithNewKeyspace();
        loadFromFile("ancestor.gql", ancestorSession);

        reachabilitySession = server.sessionWithNewKeyspace();
        loadFromFile("reachability.gql", reachabilitySession);
        reachabilitySymmetricSession = server.sessionWithNewKeyspace();
        loadFromFile("reachability-symmetric.gql", reachabilitySymmetricSession);
        sameGenerationSession = server.sessionWithNewKeyspace();
        loadFromFile("same-generation.gql", sameGenerationSession);

        final int pathSize = 2;
        final int depth = 3;
        pathTreeSession = server.sessionWithNewKeyspace();
        PathTreeGraph pathGraph = new PathTreeGraph(pathTreeSession);
        pathGraph.load(pathSize, depth);

        pathMatrixSession = server.sessionWithNewKeyspace();
        PathMatrixGraph pathMatrix = new PathMatrixGraph(pathMatrixSession);
        pathMatrix.load(pathSize, pathSize);
    }

    @AfterClass
    public static void closeSession(){
        recursivitySGSession.close();
        recursivityTCSession.close();
        recursivityRSGSession.close();

        ancestorFriendSession.close();
        ancestorSession.close();
        transitivitySession.close();

        reachabilitySession.close();
        reachabilitySymmetricSession.close();
        sameGenerationSession.close();

        pathTreeSession.close();
        pathMatrixSession.close();
    }

    private static void loadFromFile(String fileName, Session session){
        try {
            System.out.println("Loading " + fileName);
            InputStream inputStream = RecursionIT.class.getClassLoader().getResourceAsStream("test-integration/graql/reasoner/resources/recursion/" + fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            tx.graql().parser().parseList(s).forEach(Query::execute);
            tx.commit();
        } catch (Exception e){
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    /**from Vieille - Recursive Axioms in Deductive Databases p. 192*/
    @Test
    public void testTransitivity() {
        try(TransactionImpl tx = transitivitySession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);
            String queryString = "match ($x, $y) isa R;$x has index 'i'; get $y;";
            String explicitQuery = "match $y has index $ind;" +
                    "{$ind == 'j';} or {$ind == 's';} or {$ind == 'v';}; get $y;";

            assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        }
    }

    /*single-directional*/
    /**from Bancilhon - An Amateur's Introduction to Recursive Query Processing Strategies p. 25*/
    @Test
    public void testAncestor() {
        try(TransactionImpl tx = ancestorSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);

            String queryString = "match (ancestor: $X, descendant: $Y) isa Ancestor;$X has name 'aa';" +
                    "$Y has name $name;get $Y, $name;";
            String explicitQuery = "match $Y isa person, has name $name;" +
                    "{$name == 'aaa';} or {$name == 'aab';} or {$name == 'aaaa';};get $Y, $name;";

            assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        }
    }

    /**as above but both directions*/
    @Test
    public void testAncestorPrime() {
        try(TransactionImpl tx = ancestorSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);

            String queryString = "match ($X, $Y) isa Ancestor;$X has name 'aa'; get $Y;";
            String explicitQuery = "match $Y isa person, has name $name;" +
                    "{$name == 'a';} or {$name == 'aaa';} or {$name == 'aab';} or {$name == 'aaaa';};get $Y;";

            assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        }
    }

    @Test
    public void testAncestorClosure() {
        try(TransactionImpl tx = ancestorSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);

            String queryString = "match (ancestor: $X, descendant: $Y) isa Ancestor; get;";
            String explicitQuery = "match $Y isa person, has name $nameY; $X isa person, has name $nameX;" +
                    "{$nameX == 'a';$nameY == 'aa';} or {$nameX == 'a';$nameY == 'ab';} or" +
                    "{$nameX == 'a';$nameY == 'aaa';} or {$nameX == 'a';$nameY == 'aab';} or" +
                    "{$nameX == 'a';$nameY == 'aaaa';} or {$nameX == 'aa';$nameY == 'aaa';} or" +
                    "{$nameX == 'aa';$nameY == 'aab';} or {$nameX == 'aa';$nameY == 'aaaa';} or " +
                    "{$nameX == 'aaa';$nameY == 'aaaa';} or {$nameX == 'c';$nameY == 'ca';}; get $X, $Y;";

            assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        }
    }

    @Test
    public void testAncestorClosurePrime() {
        try(TransactionImpl tx = ancestorSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);
            String queryString = "match ($X, $Y) isa Ancestor; get;";
            String explicitQuery = "match $Y isa person, has name $nameY; $X isa person, has name $nameX;" +
                    "{$nameX == 'a';$nameY == 'aa';} or " +
                    "{$nameX == 'a';$nameY == 'ab';} or {$nameX == 'a';$nameY == 'aaa';} or" +
                    "{$nameX == 'a';$nameY == 'aab';} or {$nameX == 'a';$nameY == 'aaaa';} or " +
                    "{$nameY == 'a';$nameX == 'aa';} or" +
                    "{$nameY == 'a';$nameX == 'ab';} or {$nameY == 'a';$nameX == 'aaa';} or" +
                    "{$nameY == 'a';$nameX == 'aab';} or {$nameY == 'a';$nameX == 'aaaa';} or "
                    +
                    "{$nameX == 'aa';$nameY == 'aaa';} or {$nameX == 'aa';$nameY == 'aab';} or" +
                    "{$nameX == 'aa';$nameY == 'aaaa';} or " +
                    "{$nameY == 'aa';$nameX == 'aaa';} or {$nameY == 'aa';$nameX == 'aab';} or" +
                    "{$nameY == 'aa';$nameX == 'aaaa';} or "
                    +
                    "{$nameX == 'aaa';$nameY == 'aaaa';} or " +
                    "{$nameY == 'aaa';$nameX == 'aaaa';} or "
                    +
                    "{$nameX == 'c';$nameY == 'ca';} or " +
                    "{$nameY == 'c';$nameX == 'ca';}; get $X, $Y;";
            assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        }
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend() {
        try(TransactionImpl tx = ancestorFriendSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);

            String queryString = "match (ancestor: $X, ancestor-friend: $Y) isa Ancestor-friend;$X has name 'a'; $Y has name $name; get $Y, $name;";
            String explicitQuery = "match $Y has name $name;{$name == 'd';} or {$name == 'g';}; get;";

            assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        }
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriendPrime() {
        try(TransactionImpl tx = ancestorFriendSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);

            String queryString = "match ($X, $Y) isa Ancestor-friend;$X has name 'a'; get $Y;";
            String explicitQuery = "match $Y has name $name;{$name == 'd';} or {$name == 'g';}; get $Y;";

            assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        }
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend_secondVariant() {
        try(TransactionImpl tx = ancestorFriendSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);

            String queryString = "match (ancestor: $X, ancestor-friend: $Y) isa Ancestor-friend;$Y has name 'd'; get $X;";
            String explicitQuery = "match $X has name $name;" +
                    "{$name == 'a';} or {$name == 'b';} or {$name == 'c';}; get $X;";

            assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        }
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend_secondVariantPrime() {
        try(TransactionImpl tx = ancestorFriendSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);

            String queryString = "match ($X, $Y) isa Ancestor-friend;$Y has name 'd'; get $X;";
            String explicitQuery = "match $X has name $name;" +
                    "{$name == 'a';} or {$name == 'b';} or {$name == 'c';}; get $X;";

            assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        }
    }

    /**from Vieille - Recursive Query Processing: The power of logic p. 25*/
    @Test
    public void testSameGeneration(){
        try(TransactionImpl tx = recursivitySGSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);

            String queryString = "match ($x, $y) isa SameGen; $x has name 'a'; get $y;";
            String explicitQuery = "match $y has name $name;{$name == 'f';} or {$name == 'a';};get $y;";

            assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        }
    }

    /**from Vieille - Recursive Query Processing: The power of logic p. 18*/
    @Test
    public void testTC() {
        try(TransactionImpl tx = recursivityTCSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);

            String queryString = "match ($x, $y) isa N-TC; $y has index 'a'; get $x;";
            String explicitQuery = "match $x has index 'a2'; get;";

            assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        }
    }

    @Test
    public void testReachability(){
        try(TransactionImpl tx = reachabilitySession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);

            String queryString = "match (reach-from: $x, reach-to: $y) isa reachable; get;";
            String explicitQuery = "match $x has index $indX;$y has index $indY;" +
                    "{$indX == 'a';$indY == 'b';} or" +
                    "{$indX == 'b';$indY == 'c';} or" +
                    "{$indX == 'c';$indY == 'c';} or" +
                    "{$indX == 'c';$indY == 'd';} or" +
                    "{$indX == 'a';$indY == 'c';} or" +
                    "{$indX == 'b';$indY == 'd';} or" +
                    "{$indX == 'a';$indY == 'd';};get $x, $y;";

            assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        }
    }

    @Test
    public void testReachabilitySymmetric(){
        try(TransactionImpl tx = reachabilitySymmetricSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);

            String queryString = "match ($x, $y) isa reachable;$x has index 'a';get $y;";
            String explicitQuery = "match $y has index $indY;" +
                    "{$indY == 'a';} or {$indY == 'b';} or {$indY == 'c';} or {$indY == 'd';};get $y;";

            assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        }
    }

    /**test 6.6 from Cao p.76*/
    @Test
    public void testSameGenerationCao(){
        try(TransactionImpl tx = sameGenerationSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);

            String queryString = "match ($x, $y) isa SameGen;$x has name 'ann';get $y;";
            String explicitQuery = "match $y has name $name;" +
                    "{$name == 'ann';} or {$name == 'bill';} or {$name == 'peter';};get $y;";

            assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        }
    }

    /**from Abiteboul - Foundations of databases p. 312/Cao test 6.14 p. 89*/
    @Test
    public void testReverseSameGeneration(){
        try(TransactionImpl tx = recursivityRSGSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);

            String queryString = "match (RSG-from: $x, RSG-to: $y) isa RevSG;$x has name 'a'; get $y;";
            String explicitQuery = "match $y isa person, has name $name;" +
                    "{$name == 'b';} or {$name == 'c';} or {$name == 'd';};get $y;";

            assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        }
    }
    @Test
    public void testReverseSameGeneration2() {
        try(TransactionImpl tx = recursivityRSGSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);

            String queryString = "match (RSG-from: $x, RSG-to: $y) isa RevSG; get;";
            String explicitQuery = "match $x has name $nameX;$y has name $nameY;" +
                    "{$nameX == 'a';$nameY == 'b';} or {$nameX == 'a';$nameY == 'c';} or" +
                    "{$nameX == 'a';$nameY == 'd';} or {$nameX == 'm';$nameY == 'n';} or" +
                    "{$nameX == 'm';$nameY == 'o';} or {$nameX == 'p';$nameY == 'm';} or" +
                    "{$nameX == 'g';$nameY == 'f';} or {$nameX == 'h';$nameY == 'f';} or" +
                    "{$nameX == 'i';$nameY == 'f';} or {$nameX == 'j';$nameY == 'f';} or" +
                    "{$nameX == 'f';$nameY == 'k';};get $x, $y;";

            assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        }
    }

    /** test 6.1 from Cao p 71*/
    @Test
    public void testDualLinearTransitivityMatrix(){
        final int N = 5;
        Session session = server.sessionWithNewKeyspace();
        DualLinearTransitivityMatrixGraph graph = new DualLinearTransitivityMatrixGraph(session);
        graph.load(N);
        Transaction tx = session.transaction(Transaction.Type.WRITE);

        QueryBuilder qb = tx.graql().infer(false);
        QueryBuilder iqb = tx.graql().infer(true);

        String queryString = "match (Q1-from: $x, Q1-to: $y) isa Q1; $x has index 'a0'; get $y;";
        String explicitQuery = "match $y isa a-entity or $y isa end; get;";

        assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        tx.close();
        session.close();
    }

    /**
     *  test 6.3 from Cao p 75*/
    @Test
    public void testTailRecursion(){
        final int N = 10;
        final int M = 5;
        Session session = server.sessionWithNewKeyspace();
        TailRecursionGraph tailRecursionGraph = new TailRecursionGraph(session);
        tailRecursionGraph.load(N, M);
        Transaction tx = session.transaction(Transaction.Type.WRITE);

        QueryBuilder qb = tx.graql().infer(false);
        QueryBuilder iqb = tx.graql().infer(true);

        String queryString = "match (P-from: $x, P-to: $y) isa P; $x has index 'a0'; get $y;";
        String explicitQuery = "match $y isa b-entity; get;";

        assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        tx.close();
        session.close();
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
        Session session = server.sessionWithNewKeyspace();
        NguyenGraph graph = new NguyenGraph(session);
        graph.load(N);
        Transaction tx = session.transaction(Transaction.Type.WRITE);
        QueryBuilder qb = tx.graql().infer(false);
        QueryBuilder iqb = tx.graql().infer(true);

        String queryString = "match (N-rA: $x, N-rB: $y) isa N; $x has index 'c'; get $y;";
        String explicitQuery = "match $y isa a-entity; get;";

        List<ConceptMap> answers = iqb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> explicitAnswers = qb.<GetQuery>parse(explicitQuery).execute();
        assertCollectionsEqual(answers, explicitAnswers);
        tx.close();
        session.close();
    }

    /**test 6.9 from Cao p.82*/
    @Test
    public void testLinearTransitivityMatrix(){
        final int N = 5;
        final int M = 5;
        Session session = server.sessionWithNewKeyspace();
        LinearTransitivityMatrixGraph graph = new LinearTransitivityMatrixGraph(session);
        graph.load(N, M);
        Transaction tx = session.transaction(Transaction.Type.WRITE);
        QueryBuilder qb = tx.graql().infer(false);
        QueryBuilder iqb = tx.graql().infer(true);

        String queryString = "match (P-from: $x, P-to: $y) isa P;$x has index 'a'; get $y;";
        String explicitQuery = "match $y isa a-entity; get;";

        assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        tx.close();
        session.close();
    }

    /**test 6.10 from Cao p. 82*/
    @Test
    public void testPathTree(){
        TransactionImpl tx = pathTreeSession.transaction(Transaction.Type.WRITE);
        QueryBuilder qb = tx.graql().infer(false);
        QueryBuilder iqb = tx.graql().infer(true);

        Concept a0 = getConcept(tx, "index", "a0");

        String queryString = "match (path-from: $x, path-to: $y) isa path;" +
                "$x has index 'a0';" +
                "get $y;";
        String explicitQuery = "match $y isa vertex; get;";


        List<ConceptMap> answers = iqb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> explicitAnswers = qb.<GetQuery>parse(explicitQuery).execute();

        assertCollectionsEqual(answers, explicitAnswers);
    }

    @Test
    public void testPathTreePrime(){
        Transaction tx = pathTreeSession.transaction(Transaction.Type.WRITE);
        QueryBuilder qb = tx.graql().infer(false);
        QueryBuilder iqb = tx.graql().infer(true);

        String queryString = "match ($x, $y) isa path;$x has index 'a0'; get $y;";
        String explicitQuery = "match $y isa vertex; get;";

        assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
    }

    @Test
    public void testPathSymmetric(){
        final int N = 2;
        final int depth = 3;
        Session session = server.sessionWithNewKeyspace();
        PathTreeSymmetricGraph graph = new PathTreeSymmetricGraph(session);
        graph.load(N, depth);
        Transaction tx = session.transaction(Transaction.Type.WRITE);
        QueryBuilder qb = tx.graql().infer(false);
        QueryBuilder iqb = tx.graql().infer(true);

        String queryString = "match ($x, $y) isa path;$x has index 'a0'; get $y;";
        String explicitQuery = "match {$y isa vertex;} or {$y isa start-vertex;}; get;";

        assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
        tx.close();
        session.close();
    }

    @Test
    /*modified test 6.10 from Cao p. 82*/
    public void testPathMatrix(){
        final int N = 3;
        Transaction tx = pathMatrixSession.transaction(Transaction.Type.WRITE);
        QueryBuilder qb = tx.graql().infer(false);
        QueryBuilder iqb = tx.graql().infer(true);

        String queryString = "match (path-from: $x, path-to: $y) isa path;$x has index 'a0'; get $y;";
        String explicitQuery = "match $y isa vertex; get;";

        assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
    }

    @Test
    /*modified test 6.10 from Cao p. 82*/
    public void testPathMatrixPrime(){
        Transaction tx = pathTreeSession.transaction(Transaction.Type.WRITE);
        QueryBuilder qb = tx.graql().infer(false);
        QueryBuilder iqb = tx.graql().infer(true);

        String queryString = "match ($x, $y) isa path;$x has index 'a0'; $y has index $ind;get $y, $ind;";
        String explicitQuery = "match $y isa vertex;$y has index $ind; get;";

        assertQueriesEqual(qb.parse(explicitQuery), iqb.parse(queryString));
    }

    private Concept getConcept(TransactionImpl graph, String typeName, Object val){
        return graph.graql().match(Graql.var("x").has(typeName, val).admin()).get("x")
                .stream().map(ans -> ans.get("x")).findAny().orElse(null);
    }

    private static <T> void assertCollectionsEqual(Collection<T> c1, Collection<T> c2) {
        assertTrue(CollectionUtils.isEqualCollection(c1, c2));
    }

    private static void assertQueriesEqual(GetQuery q1, GetQuery q2) {
        assertCollectionsEqual(q1.execute(), q2.execute());
    }
}
