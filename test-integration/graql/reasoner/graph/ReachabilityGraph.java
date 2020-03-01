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
 * Defines a KB for the the following tests:
 * - 5.2 from Green
 * - 6.16 from Cao
 * - 6.18 from Cao
 *
 * Defines two vertex loops:
 *
 *  a -> a1 -> ... -> an -> a1
 *  b -> b1 -> ... -> bn -> b1
 *
 *  and a node configuration:
 *
 *              /^\
 *  aa -> bb -> cc -> dd
 *
 */
public class ReachabilityGraph {

    private final Session session;
    private final static String gqlPath = "test-integration/graql/reasoner/resources/recursion/";
    private final static String gqlFile = "reachability.gql";
    private final static Label key = Label.of("index");

    public ReachabilityGraph(Session session){
        this.session = session;
    }

    public final void load(int n) {
        Transaction tx = session.writeTransaction();
        loadFromFile(gqlPath, gqlFile, tx);
        buildExtensionalDB(n, tx);
        tx.commit();
    }

    protected void buildExtensionalDB(int n, Transaction tx) {
        EntityType vertex = tx.getEntityType("vertex");
        EntityType node = tx.getEntityType("node");
        Role from = tx.getRole("from");
        Role to = tx.getRole("to");
        RelationType link = tx.getRelationType("link");

        //basic test variant from Green
        putEntityWithResource(tx, "aa", node, key);
        putEntityWithResource(tx, "bb", node, key);
        putEntityWithResource(tx, "cc", node, key);
        putEntityWithResource(tx, "dd", node, key);
        link.create().assign(from, getInstance(tx, "aa")).assign(to, getInstance(tx, "bb"));
        link.create().assign(from, getInstance(tx, "bb")).assign(to, getInstance(tx, "cc"));
        link.create().assign(from, getInstance(tx, "cc")).assign(to, getInstance(tx, "cc"));
        link.create().assign(from, getInstance(tx, "cc")).assign(to, getInstance(tx, "dd"));

        //EDB for Cao tests
        putEntityWithResource(tx, "a", vertex, key);
        for(int i = 1 ; i <=n ; i++) {
            putEntityWithResource(tx, "a" + i, vertex, key);
            putEntityWithResource(tx, "b" + i, vertex, key);
        }

        link.create()
                .assign(from, getInstance(tx, "a"))
                .assign(to, getInstance(tx, "a1"));
        link.create()
                .assign(from, getInstance(tx, "a" + n))
                .assign(to, getInstance(tx, "a1"));
        link.create()
                .assign(from, getInstance(tx, "b" + n))
                .assign(to, getInstance(tx, "b1"));

        for(int i = 1 ; i < n ; i++) {
            link.create()
                    .assign(from, getInstance(tx, "a" + i))
                    .assign(to, getInstance(tx, "a" + (i + 1)));
            link.create()
                    .assign(from, getInstance(tx, "b" + i))
                    .assign(to, getInstance(tx, "b" + (i + 1)));
        }
    }
}

