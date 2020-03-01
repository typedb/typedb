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

package grakn.core.graql.reasoner.graph;

import com.google.common.math.IntMath;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;

import static grakn.core.util.GraqlTestUtil.getInstance;
import static grakn.core.util.GraqlTestUtil.loadFromFile;
import static grakn.core.util.GraqlTestUtil.putEntityWithResource;

/**
 * Defines a Graph based on test 6.10 from Cao p. 82.
 */
@SuppressWarnings("CheckReturnValue")
public class PathTreeGraph{

    private final Session session;
    private final static String gqlPath = "test-integration/graql/reasoner/resources/";
    private final String gqlFile;
    private final static Label key = Label.of("index");

    public PathTreeGraph(Session session){
        this.session = session;
        this.gqlFile = "pathTest.gql";
    }

    public PathTreeGraph(Session session, String schemaFile) {
        this.session = session;
        this.gqlFile = schemaFile;
    }

    public final void load(int n, int m) {
        Transaction tx = session.writeTransaction();
        loadFromFile(gqlPath, gqlFile, tx);
        buildExtensionalDB(n, m, tx);
        tx.commit();
    }

    protected void buildExtensionalDB(int n, int children, Transaction tx) {
        buildTree("arc-from", "arc-to", n , children, tx);
    }

    void buildTree(String fromRoleValue, String toRoleValue, int n, int children, Transaction tx) {
        Role fromRole = tx.getRole(fromRoleValue);
        Role toRole = tx.getRole(toRoleValue);

        EntityType vertex = tx.getEntityType("vertex");
        EntityType startVertex = tx.getEntityType("start-vertex");

        RelationType arc = tx.getRelationType("arc");
        putEntityWithResource(tx, "a0", startVertex, key);

        int outputThreshold = 500;
        for (int i = 1; i <= n; i++) {
            int m = IntMath.pow(children, i);
            for (int j = 0; j < m; j++) {
                putEntityWithResource(tx, "a" + i + "," + j, vertex, key);
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
}
