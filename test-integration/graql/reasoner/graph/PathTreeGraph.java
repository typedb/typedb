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

package grakn.core.graql.reasoner.graph;

import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import com.google.common.math.IntMath;

/**
 * Defines a Graph based on test 6.10 from Cao p. 82.
 */
@SuppressWarnings("CheckReturnValue")
public class PathTreeGraph extends ParametrisedTestGraph {

    public PathTreeGraph(Session session) {
        super(session, "pathTest.gql", Label.of("index"));
    }

    public PathTreeGraph(Session session, String schemaFile) {
        super(session, schemaFile, Label.of("index"));
    }

    protected void buildExtensionalDB(int n, int children, Transaction tx) {
        buildTree("arc-from", "arc-to", n , children, tx);
    }

    void buildTree(String fromRoleValue, String toRoleValue, int n, int children, Transaction tx) {
        Role fromRole = tx.getRole(fromRoleValue);
        Role toRole = tx.getRole(toRoleValue);

        EntityType vertex = tx.getEntityType("vertex");
        EntityType startVertex = tx.getEntityType("start-vertex");

        RelationshipType arc = tx.getRelationshipType("arc");
        putEntityWithResource(tx, "a0", startVertex, key());

        int outputThreshold = 500;
        for (int i = 1; i <= n; i++) {
            int m = IntMath.pow(children, i);
            for (int j = 0; j < m; j++) {
                putEntityWithResource(tx, "a" + i + "," + j, vertex, key());
                if (j != 0 && j % outputThreshold == 0) {
                    System.out.println(j + " entities out of " + m + " inserted");
                }
            }
        }

        for (int j = 0; j < children; j++) {
            arc.create()
                    .assign(fromRole, getInstance(tx, "a0"))
                    .assign(toRole, getInstance(tx, "a1," + j));
        }

        for (int i = 1; i < n; i++) {
            int m = IntMath.pow(children, i);
            for (int j = 0; j < m; j++) {
                for (int c = 0; c < children; c++) {
                    arc.create()
                            .assign(fromRole, getInstance(tx, "a" + i + "," + j))
                            .assign(toRole, getInstance(tx, "a" + (i + 1) + "," + (j * children + c)));

                }
                if (j != 0 && j % outputThreshold == 0) {
                    System.out.println("level " + i + "/" + (n - 1) + ": " + j + " entities out of " + m + " connected");
                }
            }
        }
    }

    @Override
    protected void buildExtensionalDB(int n, Transaction tx) {
        buildExtensionalDB(n, n, tx);
    }
}
