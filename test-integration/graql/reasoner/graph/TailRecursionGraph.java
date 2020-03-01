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

@SuppressWarnings("CheckReturnValue")
public class TailRecursionGraph{

    private final Session session;
    private final static String gqlPath = "test-integration/graql/reasoner/resources/recursion/";
    private final static String gqlFile = "tail-recursion.gql";
    private final static Label key = Label.of("index");

    public TailRecursionGraph(Session session){
        this.session = session;
    }

    public final void load(int n, int m) {
        Transaction tx = session.writeTransaction();
        loadFromFile(gqlPath, gqlFile, tx);
        buildExtensionalDB(n, m, tx);
        tx.commit();
    }

    protected void buildExtensionalDB(int n, int m, Transaction tx) {
        Role qfrom = tx.getRole("Q-from");
        Role qto = tx.getRole("Q-to");

        EntityType aEntity = tx.getEntityType("a-entity");
        EntityType bEntity = tx.getEntityType("b-entity");
        RelationType q = tx.getRelationType("Q");

        putEntityWithResource(tx, "a0", aEntity, key);
        for(int i = 1 ; i <= m + 1 ;i++) {
            for (int j = 1; j <= n; j++) {
                putEntityWithResource(tx, "b" + i + "," + j, bEntity, key);
            }
        }

        for (int j = 1; j <= n; j++) {
            q.create()
                    .assign(qfrom, getInstance(tx, "a0"))
                    .assign(qto, getInstance(tx, "b1" + "," + j));
            for(int i = 1 ; i <= m ;i++) {
                q.create()
                        .assign(qfrom, getInstance(tx, "b" + i + "," + j))
                        .assign(qto, getInstance(tx, "b" + (i + 1) + "," + j));
            }
        }
    }
}
