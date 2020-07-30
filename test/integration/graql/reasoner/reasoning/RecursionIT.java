/*
 * Copyright (C) 2020 Grakn Labs
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

import grakn.core.graql.reasoner.graph.LinearTransitivityMatrixGraph;
import grakn.core.graql.reasoner.graph.PathMatrixGraph;
import grakn.core.graql.reasoner.graph.PathTreeGraph;
import grakn.core.graql.reasoner.graph.PathTreeSymmetricGraph;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestServer;
import grakn.core.test.common.GraqlTestUtil;
import graql.lang.Graql;
import org.junit.ClassRule;
import org.junit.Test;

@SuppressWarnings("CheckReturnValue")
public class RecursionIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

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

                GraqlTestUtil.assertCollectionsNonTriviallyEqual(tx.execute(Graql.parse(explicitQuery).asGet(), false), tx.execute(Graql.parse(queryString).asGet()));
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
                GraqlTestUtil.assertCollectionsNonTriviallyEqual(tx.execute(Graql.parse(explicitQuery).asGet(), false), tx.execute(Graql.parse(query).asGet()));

                String noRoleQuery = "match ($x, $y) isa path;$x has index 'a0'; get $y;";
                GraqlTestUtil.assertCollectionsNonTriviallyEqual(tx.execute(Graql.parse(explicitQuery).asGet(), false), tx.execute(Graql.parse(noRoleQuery).asGet()));
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

                GraqlTestUtil.assertCollectionsNonTriviallyEqual(tx.execute(Graql.parse(explicit).asGet(), false), tx.execute(Graql.parse(query).asGet()));

                String noRoleQuery = "match ($x, $y) isa path;$x has index 'a0'; $y has index $ind;get $y, $ind;";
                String explicitWithIndices = "match $y isa vertex;$y has index $ind; get;";

                GraqlTestUtil.assertCollectionsNonTriviallyEqual(tx.execute(Graql.parse(explicitWithIndices).asGet(), false), tx.execute(Graql.parse(noRoleQuery).asGet()));
            }
        }
    }
}
