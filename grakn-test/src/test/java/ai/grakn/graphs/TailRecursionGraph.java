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

package ai.grakn.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeLabel;

import java.util.function.Consumer;

public class TailRecursionGraph extends TestGraph {

    private final static TypeLabel key = TypeLabel.of("index");
    private final static String gqlFile = "tail-recursion-test.gql";

    private final int n;
    private final int m;

    public TailRecursionGraph(int n, int m) {
        this.n = n;
        this.m = m;
    }

    public static Consumer<GraknGraph> get(int n, int m) {
        return new TailRecursionGraph(n, m).build();
    }

    @Override
    public Consumer<GraknGraph> build(){
        return (GraknGraph graph) -> {
            loadFromFile(graph, gqlFile);
            buildExtensionalDB(graph, n, m);
        };
    }

    private void buildExtensionalDB(GraknGraph graph, int n, int m) {
        RoleType Qfrom = graph.getRoleType("Q-from");
        RoleType Qto = graph.getRoleType("Q-to");

        EntityType aEntity = graph.getEntityType("a-entity");
        EntityType bEntity = graph.getEntityType("b-entity");
        RelationType Q = graph.getRelationType("Q");

        putEntity(graph, "a0", aEntity, key);
        for(int i = 1 ; i <= m + 1 ;i++)
            for(int j = 1 ; j <= n ;j++)
                putEntity(graph, "b" + i + "," + j, bEntity, key);

        for (int j = 1; j <= n; j++) {
            Q.addRelation()
                    .addRolePlayer(Qfrom, getInstance(graph, "a0"))
                    .addRolePlayer(Qto, getInstance(graph, "b1" + "," + j));
            for(int i = 1 ; i <= m ;i++) {
                Q.addRelation()
                        .addRolePlayer(Qfrom, getInstance(graph, "b" + i + "," + j))
                        .addRolePlayer(Qto, getInstance(graph, "b" + (i + 1) + "," + j));
            }
        }
    }
}
