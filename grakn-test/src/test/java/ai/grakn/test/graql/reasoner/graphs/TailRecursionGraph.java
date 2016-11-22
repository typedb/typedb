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

package ai.grakn.test.graql.reasoner.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RoleType;

public class TailRecursionGraph extends TestGraph {

    final static String key = "index";
    final static String gqlFile = "tail-recursion-test.gql";

    public TailRecursionGraph(int n, int m) {
        super(key, gqlFile);
        buildExtensionalDB(n, m);
        commit();
    }

    public static GraknGraph getGraph(int n, int m) {
        return new TailRecursionGraph(n, m).graph();
    }

    private void buildExtensionalDB(int n, int m) {
        RoleType Qfrom = graknGraph.getRoleType("Q-from");
        RoleType Qto = graknGraph.getRoleType("Q-to");

        EntityType aEntity = graknGraph.getEntityType("a-entity");
        EntityType bEntity = graknGraph.getEntityType("b-entity");
        RelationType Q = graknGraph.getRelationType("Q");

        String a0Id = putEntity("a0", aEntity).getId();
        String[][] bInstancesIds = new String[m + 2][n + 2];
        for (int i = 1; i <= m; i++)
            for (int j = 1; j <= n; j++)
                bInstancesIds[i][j] = putEntity("b" + i + j, bEntity).getId();

        for (int j = 1; j <= n; j++) {
            Q.addRelation()
                    .putRolePlayer(Qfrom, graknGraph.getConcept(a0Id))
                    .putRolePlayer(Qto, graknGraph.getConcept(bInstancesIds[1][j]));
            for (int i = 1; i <= m; i++) {
                Q.addRelation()
                        .putRolePlayer(Qfrom, graknGraph.getConcept(bInstancesIds[i][j]))
                        .putRolePlayer(Qto, graknGraph.getConcept(bInstancesIds[i + 1][j]));
            }
        }
    }
}
