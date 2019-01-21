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

package grakn.core.graql.reasoner.reasoning;

import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.Graql;
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
import grakn.core.util.GraqlTestUtil;
import org.junit.ClassRule;
import org.junit.Test;

@SuppressWarnings("CheckReturnValue")
public class RecursionIT {

    private static String resourcePath = "test-integration/graql/reasoner/resources/recursion/";

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    /**
     * from Vieille - Recursive Axioms in Deductive Databases p. 192
     */
    @Test
    public void testTransitivity() {
        try (Session session = server.sessionWithNewKeyspace()) {
            GraqlTestUtil.loadFromFileAndCommit(resourcePath, "transitivity.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                String queryString = "match ($x, $y) isa R;$x has index 'i'; get $y;";
                String explicitQuery = "match $y has index $ind;" +
                        "{$ind == 'j';} or {$ind == 's';} or {$ind == 'v';}; get $y;";

                GraqlTestUtil.assertCollectionsEqual(
                        tx.execute(Graql.<GetQuery>parse(explicitQuery), false),
                        tx.execute(Graql.<GetQuery>parse(queryString))
                );
            }
        }
    }

    /*single-directional*/

    /**
     * from Bancilhon - An Amateur's Introduction to Recursive Query Processing Strategies p. 25
     */
    @Test
    public void testAncestor() {
        try (Session session = server.sessionWithNewKeyspace()) {
            GraqlTestUtil.loadFromFileAndCommit(resourcePath, "ancestor.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                String query = "match (ancestor: $X, descendant: $Y) isa Ancestor;$X has name 'aa';" +
                        "$Y has name $name;get $Y, $name;";
                String explicitQuery = "match $Y isa person, has name $name;" +
                        "{$name == 'aaa';} or {$name == 'aab';} or {$name == 'aaaa';};get $Y, $name;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitQuery), false), tx.execute(Graql.<GetQuery>parse(query)));

                String noRoleQuery = "match ($X, $Y) isa Ancestor;$X has name 'aa'; get $Y;";
                String explicitQuery2 = "match $Y isa person, has name $name;" +
                        "{$name == 'a';} or {$name == 'aaa';} or {$name == 'aab';} or {$name == 'aaaa';};get $Y;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitQuery2), false), tx.execute(Graql.<GetQuery>parse(noRoleQuery)));

                String closure = "match (ancestor: $X, descendant: $Y) isa Ancestor; get;";
                String explicitClosure = "match $Y isa person, has name $nameY; $X isa person, has name $nameX;" +
                        "{$nameX == 'a';$nameY == 'aa';} or {$nameX == 'a';$nameY == 'ab';} or" +
                        "{$nameX == 'a';$nameY == 'aaa';} or {$nameX == 'a';$nameY == 'aab';} or" +
                        "{$nameX == 'a';$nameY == 'aaaa';} or {$nameX == 'aa';$nameY == 'aaa';} or" +
                        "{$nameX == 'aa';$nameY == 'aab';} or {$nameX == 'aa';$nameY == 'aaaa';} or " +
                        "{$nameX == 'aaa';$nameY == 'aaaa';} or {$nameX == 'c';$nameY == 'ca';}; get $X, $Y;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitClosure), false), tx.execute(Graql.<GetQuery>parse(closure)));

                String noRoleClosure = "match ($X, $Y) isa Ancestor; get;";
                String explicitNoRoleClosure = "match $Y isa person, has name $nameY; $X isa person, has name $nameX;" +
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
                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitNoRoleClosure), false), tx.execute(Graql.<GetQuery>parse(noRoleClosure)));
            }
        }
    }

    /**
     * from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186
     */
    @Test
    public void testAncestorFriend() {
        try (Session session = server.sessionWithNewKeyspace()) {
            GraqlTestUtil.loadFromFileAndCommit(resourcePath, "ancestor-friend.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                String ancestorVariant = "match (ancestor: $X, ancestor-friend: $Y) isa Ancestor-friend;$X has name 'a'; $Y has name $name; get $Y;";
                String explicitAncestorQuery = "match $Y has name $name;{$name == 'd';} or {$name == 'g';}; get $Y;";
                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitAncestorQuery), false), tx.execute(Graql.<GetQuery>parse(ancestorVariant)));

                String noRoleAncestorQuery = "match ($X, $Y) isa Ancestor-friend;$X has name 'a'; get $Y;";
                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitAncestorQuery), false), tx.execute(Graql.<GetQuery>parse(noRoleAncestorQuery)));

                String ancestorFriendVariant = "match (ancestor: $X, ancestor-friend: $Y) isa Ancestor-friend;$Y has name 'd'; get $X;";
                String explicitAncestorFriendQuery = "match $X has name $name;" +
                        "{$name == 'a';} or {$name == 'b';} or {$name == 'c';}; get $X;";
                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitAncestorFriendQuery), false), tx.execute(Graql.<GetQuery>parse(ancestorFriendVariant)));

                String noRoleAncestorFriendQuery = "match ($X, $Y) isa Ancestor-friend;$Y has name 'd'; get $X;";
                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitAncestorFriendQuery), false), tx.execute(Graql.<GetQuery>parse(noRoleAncestorFriendQuery)));
            }
        }
    }

    /**
     * from Vieille - Recursive Query Processing: The power of logic p. 25
     */
    @Test
    public void testSameGeneration() {
        try (Session session = server.sessionWithNewKeyspace()) {
            GraqlTestUtil.loadFromFileAndCommit(resourcePath, "recursivity-sg.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                String queryString = "match ($x, $y) isa SameGen; $x has name 'a'; get $y;";
                String explicitQuery = "match $y has name $name;{$name == 'f';} or {$name == 'a';};get $y;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitQuery), false), tx.execute(Graql.<GetQuery>parse(queryString)));
            }
        }
    }

    /**
     * from Vieille - Recursive Query Processing: The power of logic p. 18
     */
    @Test
    public void testTC() {
        try (Session session = server.sessionWithNewKeyspace()) {
            GraqlTestUtil.loadFromFileAndCommit(resourcePath, "recursivity-tc.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                String queryString = "match ($x, $y) isa N-TC; $y has index 'a'; get $x;";
                String explicitQuery = "match $x has index 'a2'; get;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitQuery), false), tx.execute(Graql.<GetQuery>parse(queryString)));
            }
        }
    }

    @Test
    public void testReachability() {
        try (Session session = server.sessionWithNewKeyspace()) {
            GraqlTestUtil.loadFromFileAndCommit(resourcePath, "reachability.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                String queryString = "match (reach-from: $x, reach-to: $y) isa reachable; get;";
                String explicitQuery = "match $x has index $indX;$y has index $indY;" +
                        "{$indX == 'a';$indY == 'b';} or" +
                        "{$indX == 'b';$indY == 'c';} or" +
                        "{$indX == 'c';$indY == 'c';} or" +
                        "{$indX == 'c';$indY == 'd';} or" +
                        "{$indX == 'a';$indY == 'c';} or" +
                        "{$indX == 'b';$indY == 'd';} or" +
                        "{$indX == 'a';$indY == 'd';};get $x, $y;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitQuery), false), tx.execute(Graql.<GetQuery>parse(queryString)));
            }
        }
    }

    @Test
    public void testReachabilitySymmetric() {
        try (Session session = server.sessionWithNewKeyspace()) {
            GraqlTestUtil.loadFromFileAndCommit(resourcePath, "reachability-symmetric.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                String queryString = "match ($x, $y) isa reachable;$x has index 'a';get $y;";
                String explicitQuery = "match $y has index $indY;" +
                        "{$indY == 'a';} or {$indY == 'b';} or {$indY == 'c';} or {$indY == 'd';};get $y;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitQuery), false), tx.execute(Graql.<GetQuery>parse(queryString)));
            }
        }
    }

    /**
     * test 6.6 from Cao p.76
     */
    @Test
    public void testSameGenerationCao() {
        try (Session session = server.sessionWithNewKeyspace()) {
            GraqlTestUtil.loadFromFileAndCommit(resourcePath, "same-generation.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                String queryString = "match ($x, $y) isa SameGen;$x has name 'ann';get $y;";
                String explicitQuery = "match $y has name $name;" +
                        "{$name == 'ann';} or {$name == 'bill';} or {$name == 'peter';};get $y;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitQuery), false), tx.execute(Graql.<GetQuery>parse(queryString)));
            }
        }
    }

    /**
     * from Abiteboul - Foundations of databases p. 312/Cao test 6.14 p. 89
     */
    @Test
    public void testReverseSameGeneration() {
        try (Session session = server.sessionWithNewKeyspace()) {
            GraqlTestUtil.loadFromFileAndCommit(resourcePath, "recursivity-rsg.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                String specificQuery = "match (RSG-from: $x, RSG-to: $y) isa RevSG;$x has name 'a'; get $y;";
                String explicitQuery = "match $y isa person, has name $name;" +
                        "{$name == 'b';} or {$name == 'c';} or {$name == 'd';};get $y;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitQuery), false), tx.execute(Graql.<GetQuery>parse(specificQuery)));

                String generalQuery = "match (RSG-from: $x, RSG-to: $y) isa RevSG; get;";
                String generalExplicitQuery = "match $x has name $nameX;$y has name $nameY;" +
                        "{$nameX == 'a';$nameY == 'b';} or {$nameX == 'a';$nameY == 'c';} or" +
                        "{$nameX == 'a';$nameY == 'd';} or {$nameX == 'm';$nameY == 'n';} or" +
                        "{$nameX == 'm';$nameY == 'o';} or {$nameX == 'p';$nameY == 'm';} or" +
                        "{$nameX == 'g';$nameY == 'f';} or {$nameX == 'h';$nameY == 'f';} or" +
                        "{$nameX == 'i';$nameY == 'f';} or {$nameX == 'j';$nameY == 'f';} or" +
                        "{$nameX == 'f';$nameY == 'k';};get $x, $y;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(generalExplicitQuery), false), tx.execute(Graql.<GetQuery>parse(generalQuery)));
            }
        }
    }

    /**
     * test 6.1 from Cao p 71
     */
    @Test
    public void testDualLinearTransitivityMatrix() {
        final int N = 5;
        try (Session session = server.sessionWithNewKeyspace()) {
            DualLinearTransitivityMatrixGraph graph = new DualLinearTransitivityMatrixGraph(session);
            graph.load(N, N);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                String queryString = "match (Q1-from: $x, Q1-to: $y) isa Q1; $x has index 'a0'; get $y;";
                String explicitQuery = "match { $y isa a-entity; } or { $y isa end; }; get;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitQuery), false), tx.execute(Graql.<GetQuery>parse(queryString)));
            }
        }
    }

    /**
     * test 6.3 from Cao p 75
     */
    @Test
    public void testTailRecursion() {
        final int N = 10;
        final int M = 5;
        try (Session session = server.sessionWithNewKeyspace()) {
            TailRecursionGraph tailRecursionGraph = new TailRecursionGraph(session);
            tailRecursionGraph.load(N, M);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                String queryString = "match (P-from: $x, P-to: $y) isa P; $x has index 'a0'; get $y;";
                String explicitQuery = "match $y isa b-entity; get;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitQuery), false), tx.execute(Graql.<GetQuery>parse(queryString)));
            }
        }
    }

    /**
     * test3 from Nguyen (similar to test 6.5 from Cao):
     * <p>
     * N(x, y) :- R(x, y)
     * N(x, y) :- P(x, z), N(z, w), Q(w, y)
     * <p>
     * <p>
     * c -- P -- d -- R -- e -- Q -- a0
     * \                        /
     * P               Q
     * \    \          /
     * b0   --  Q  --   a1
     * \                     /
     * P              Q
     * \        /
     * b1   --  Q  --   a2
     * .
     * .
     * .
     * bN   --  Q --    aN+1
     */
    @Test
    public void testNguyen() {
        final int N = 9;
        try (Session session = server.sessionWithNewKeyspace()) {
            NguyenGraph graph = new NguyenGraph(session);
            graph.load(N);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                String queryString = "match (N-rA: $x, N-rB: $y) isa N; $x has index 'c'; get $y;";
                String explicitQuery = "match $y isa a-entity; get;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitQuery), false), tx.execute(Graql.<GetQuery>parse(queryString)));
            }
        }
    }

    /**
     * test 6.9 from Cao p.82
     */
    @Test
    public void testLinearTransitivityMatrix() {
        final int N = 5;
        final int M = 5;
        try (Session session = server.sessionWithNewKeyspace()) {
            LinearTransitivityMatrixGraph graph = new LinearTransitivityMatrixGraph(session);
            graph.load(N, M);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                String queryString = "match (P-from: $x, P-to: $y) isa P; $x has index 'a'; get $y;";
                String explicitQuery = "match $y isa a-entity; get;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitQuery), false), tx.execute(Graql.<GetQuery>parse(queryString)));
            }
        }
    }

    @Test
    public void testPathSymmetric() {
        final int N = 2;
        final int depth = 3;
        try (Session session = server.sessionWithNewKeyspace()) {
            PathTreeSymmetricGraph graph = new PathTreeSymmetricGraph(session);
            graph.load(N, depth);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                String queryString = "match ($x, $y) isa path;$x has index 'a0'; get $y;";
                String explicitQuery = "match {$y isa vertex;} or {$y isa start-vertex;}; get;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitQuery), false), tx.execute(Graql.<GetQuery>parse(queryString)));
            }
        }
    }

    /**
     * test 6.10 from Cao p. 82
     */
    @Test
    public void testPathTree() {
        try (Session session = server.sessionWithNewKeyspace()) {
            final int N = 2;
            final int depth = 3;
            PathTreeGraph graph = new PathTreeGraph(session);
            graph.load(N, depth);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                String query = "match (path-from: $x, path-to: $y) isa path;" +
                        "$x has index 'a0';" +
                        "get $y;";
                String explicitQuery = "match $y isa vertex; get;";
                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitQuery), false), tx.execute(Graql.<GetQuery>parse(query)));

                String noRoleQuery = "match ($x, $y) isa path;$x has index 'a0'; get $y;";
                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitQuery), false), tx.execute(Graql.<GetQuery>parse(noRoleQuery)));
            }
        }
    }

    @Test
    /*modified test 6.10 from Cao p. 82*/
    public void testPathMatrix() {
        try (Session session = server.sessionWithNewKeyspace()) {
            final int pathSize = 2;
            PathMatrixGraph graph = new PathMatrixGraph(session);
            graph.load(pathSize, pathSize);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                String query = "match (path-from: $x, path-to: $y) isa path;$x has index 'a0'; get $y;";
                String explicit = "match $y isa vertex; get;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicit), false), tx.execute(Graql.<GetQuery>parse(query)));

                String noRoleQuery = "match ($x, $y) isa path;$x has index 'a0'; $y has index $ind;get $y, $ind;";
                String explicitWithIndices = "match $y isa vertex;$y has index $ind; get;";

                GraqlTestUtil.assertCollectionsEqual(tx.execute(Graql.<GetQuery>parse(explicitWithIndices), false), tx.execute(Graql.<GetQuery>parse(noRoleQuery)));
            }
        }
    }
}
