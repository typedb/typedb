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
 * Defines a KB based on test 6.10 from Cao p. 82, but arranged in a matrix instead of a tree.
 */
@SuppressWarnings("CheckReturnValue")
public class PathMatrixGraph {

    private final Session session;
    private final static String gqlPath = "test-integration/graql/reasoner/resources/";
    private final static String gqlFile = "pathTest.gql";
    private final static Label key = Label.of("index");

    public PathMatrixGraph(Session session){
        this.session = session;
    }

    public final void load(int n, int m) {
        Transaction tx = session.writeTransaction();
        loadFromFile(gqlPath, gqlFile, tx);
        buildExtensionalDB(n, m, tx);
        tx.commit();
    }

    protected void buildExtensionalDB(int n, int m, Transaction tx) {
        EntityType vertex = tx.getEntityType("vertex");
        EntityType startVertex = tx.getEntityType("start-vertex");
        Role arcFrom = tx.getRole("arc-from");
        Role arcTo = tx.getRole("arc-to");

        RelationType arc = tx.getRelationType("arc");
        putEntityWithResource(tx, "a0", startVertex, key);

        for(int i = 0 ; i < n ;i++) {
            for (int j = 0; j < m; j++) {
                putEntityWithResource(tx, "a" + i + "," + j, vertex, key);
            }
        }

        arc.create()
                .assign(arcFrom, getInstance(tx, "a0"))
                .assign(arcTo, getInstance(tx, "a0,0"));

        for(int i = 0 ; i < n ;i++) {
            for (int j = 0; j < m; j++) {
                if (j < n - 1) {
                    arc.create()
                            .assign(arcFrom, getInstance(tx, "a" + i + "," + j))
                            .assign(arcTo, getInstance(tx, "a" + i + "," + (j + 1)));
                }
                if (i < m - 1) {
                    arc.create()
                            .assign(arcFrom, getInstance(tx, "a" + i + "," + j))
                            .assign(arcTo, getInstance(tx, "a" + (i + 1) + "," + j));
                }
            }
        }
    }
}
