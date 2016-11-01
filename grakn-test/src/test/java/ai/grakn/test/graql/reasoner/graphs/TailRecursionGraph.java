/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.graql.reasoner.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;

public class TailRecursionGraph extends GenericGraph{

    public static GraknGraph getGraph(int n, int m) {
        final String gqlFile = "tail-recursion-test.gql";
        getGraph(gqlFile);
        buildExtensionalDB(n, m);
        commit();
        return grakn;
    }

    private static void buildExtensionalDB(int n, int m) {
        RoleType Qfrom = grakn.getRoleType("Q-from");
        RoleType Qto = grakn.getRoleType("Q-to");

        EntityType aEntity = grakn.getEntityType("a-entity");
        EntityType bEntity = grakn.getEntityType("b-entity");
        RelationType Q = grakn.getRelationType("Q");

        grakn.putEntity("a0", aEntity);
        for(int i = 1 ; i <= m ;i++)
            for(int j = 1 ; j <= n ;j++)
                grakn.putEntity("b" + i + j, bEntity);

        for (int j = 1; j <= n; j++) {

            grakn.addRelation(Q)
                    .putRolePlayer(Qfrom, grakn.getInstance("a0"))
                    .putRolePlayer(Qto, grakn.getInstance("b1" + j));
            for(int i = 1 ; i <= m ;i++) {
                grakn.addRelation(Q)
                        .putRolePlayer(Qfrom, grakn.getInstance("b" + i + j))
                        .putRolePlayer(Qto, grakn.getInstance("b" + (i + 1) + j));
            }
        }

    }
}
