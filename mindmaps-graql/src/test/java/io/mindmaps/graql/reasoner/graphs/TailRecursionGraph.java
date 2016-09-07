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

package io.mindmaps.graql.reasoner.graphs;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.RoleType;

public class TailRecursionGraph extends GenericGraph{

    public static MindmapsGraph getGraph(int n, int m) {
        final String gqlFile = "tail-recursion-test.gql";
        getGraph(gqlFile);
        buildExtensionalDB(n, m);
        commit();
        return mindmaps;
    }

    private static void buildExtensionalDB(int n, int m) {
        RoleType Qfrom = mindmaps.getRoleType("Q-from");
        RoleType Qto = mindmaps.getRoleType("Q-to");

        EntityType aEntity = mindmaps.getEntityType("a-entity");
        EntityType bEntity = mindmaps.getEntityType("b-entity");
        RelationType Q = mindmaps.getRelationType("Q");

        mindmaps.putEntity("a0", aEntity);
        for(int i = 1 ; i <= m ;i++)
            for(int j = 1 ; j <= n ;j++)
                mindmaps.putEntity("b" + i + j, bEntity);

        for (int j = 1; j <= n; j++) {

            mindmaps.addRelation(Q)
                    .putRolePlayer(Qfrom, mindmaps.getInstance("a0"))
                    .putRolePlayer(Qto, mindmaps.getInstance("b1" + j));
            for(int i = 1 ; i <= m ;i++) {
                mindmaps.addRelation(Q)
                        .putRolePlayer(Qfrom, mindmaps.getInstance("b" + i + j))
                        .putRolePlayer(Qto, mindmaps.getInstance("b" + (i + 1) + j));
            }
        }

    }
}
