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

public class MatrixGraph extends GenericGraph{

    public static MindmapsGraph getGraph(int n, int m) {
        final String gqlFile = "matrix-test.gql";
        getGraph(gqlFile);
        buildExtensionalDB(n, m);
        commit();
        return mindmaps;
    }

    private static void buildExtensionalDB(int n, int m) {
        RoleType R1from = mindmaps.getRoleType("R1-from");
        RoleType R1to = mindmaps.getRoleType("R1-to");
        RoleType R2from = mindmaps.getRoleType("R2-from");
        RoleType R2to = mindmaps.getRoleType("R2-to");

        EntityType aEntity = mindmaps.getEntityType("a-entity");
        EntityType bEntity = mindmaps.getEntityType("b-entity");
        RelationType R1 = mindmaps.getRelationType("R1");
        RelationType R2 = mindmaps.getRelationType("R2");

        for(int i = 0 ; i <= m ;i++)
            mindmaps.putEntity("a" + i, aEntity);

        for(int i = 1 ; i < m ;i++)
            for(int j = 1 ; j <= n ;j++)
                mindmaps.putEntity("b" + i + j, bEntity);

        for (int i = 0; i < m; i++)
            mindmaps.addRelation(R1)
                    .putRolePlayer(R1from, mindmaps.getInstance("a" + i))
                    .putRolePlayer(R1to, mindmaps.getInstance("a" + (i+1)));

        for(int j = 1 ; j <= n ;j++) {
            mindmaps.addRelation(R2)
                    .putRolePlayer(R2from, mindmaps.getInstance("a0"))
                    .putRolePlayer(R2to, mindmaps.getInstance("b1" + j));
            mindmaps.addRelation(R2)
                    .putRolePlayer(R2from, mindmaps.getInstance("b" + (m-1) + j))
                    .putRolePlayer(R2to, mindmaps.getInstance("a" + m));
            for (int i = 1; i < m - 1; i++) {
                mindmaps.addRelation(R2)
                        .putRolePlayer(R2from, mindmaps.getInstance("b" + i + j))
                        .putRolePlayer(R2to, mindmaps.getInstance("b" + (i+1) + j));
            }
        }
    }
}
