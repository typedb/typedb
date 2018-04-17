/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.test.kbs;

import ai.grakn.GraknTx;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.SampleKBLoader;

import java.util.function.Consumer;

/**
 *
 * @author Kasper Piskorski
 *
 */
public class TailRecursionKB extends TestKB {

    private final static Label key = Label.of("index");
    private final static String gqlFile = "tail-recursion-test.gql";

    private final int n;
    private final int m;

    public TailRecursionKB(int n, int m) {
        this.n = n;
        this.m = m;
    }

    public static SampleKBContext context(int n, int m) {
        return new TailRecursionKB(n, m).makeContext();
    }

    @Override
    public Consumer<GraknTx> build(){
        return (GraknTx graph) -> {
            SampleKBLoader.loadFromFile(graph, gqlFile);
            buildExtensionalDB(graph, n, m);
        };
    }

    private void buildExtensionalDB(GraknTx graph, int n, int m) {
        Role qfrom = graph.getRole("Q-from");
        Role qto = graph.getRole("Q-to");

        EntityType aEntity = graph.getEntityType("a-entity");
        EntityType bEntity = graph.getEntityType("b-entity");
        RelationshipType q = graph.getRelationshipType("Q");

        putEntityWithResource(graph, "a0", aEntity, key);
        for(int i = 1 ; i <= m + 1 ;i++) {
            for (int j = 1; j <= n; j++) {
                putEntityWithResource(graph, "b" + i + "," + j, bEntity, key);
            }
        }

        for (int j = 1; j <= n; j++) {
            q.addRelationship()
                    .addRolePlayer(qfrom, getInstance(graph, "a0"))
                    .addRolePlayer(qto, getInstance(graph, "b1" + "," + j));
            for(int i = 1 ; i <= m ;i++) {
                q.addRelationship()
                        .addRolePlayer(qfrom, getInstance(graph, "b" + i + "," + j))
                        .addRolePlayer(qto, getInstance(graph, "b" + (i + 1) + "," + j));
            }
        }
    }
}
