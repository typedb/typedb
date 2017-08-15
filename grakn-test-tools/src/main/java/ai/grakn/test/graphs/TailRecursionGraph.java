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

package ai.grakn.test.graphs;

import ai.grakn.GraknTx;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.test.GraphContext;

import java.util.function.Consumer;

/**
 *
 * @author Kasper Piskorski
 *
 */
public class TailRecursionGraph extends TestGraph {

    private final static Label key = Label.of("index");
    private final static String gqlFile = "tail-recursion-test.gql";

    private final int n;
    private final int m;

    public TailRecursionGraph(int n, int m) {
        this.n = n;
        this.m = m;
    }

    public static Consumer<GraknTx> get(int n, int m) {
        return new TailRecursionGraph(n, m).build();
    }

    @Override
    public Consumer<GraknTx> build(){
        return (GraknTx graph) -> {
            GraphContext.loadFromFile(graph, gqlFile);
            buildExtensionalDB(graph, n, m);
        };
    }

    private void buildExtensionalDB(GraknTx graph, int n, int m) {
        Role qfrom = graph.getRole("Q-from");
        Role qto = graph.getRole("Q-to");

        EntityType aEntity = graph.getEntityType("a-entity");
        EntityType bEntity = graph.getEntityType("b-entity");
        RelationType q = graph.getRelationType("Q");

        putEntity(graph, "a0", aEntity, key);
        for(int i = 1 ; i <= m + 1 ;i++) {
            for (int j = 1; j <= n; j++) {
                putEntity(graph, "b" + i + "," + j, bEntity, key);
            }
        }

        for (int j = 1; j <= n; j++) {
            q.addRelation()
                    .addRolePlayer(qfrom, getInstance(graph, "a0"))
                    .addRolePlayer(qto, getInstance(graph, "b1" + "," + j));
            for(int i = 1 ; i <= m ;i++) {
                q.addRelation()
                        .addRolePlayer(qfrom, getInstance(graph, "b" + i + "," + j))
                        .addRolePlayer(qto, getInstance(graph, "b" + (i + 1) + "," + j));
            }
        }
    }
}
