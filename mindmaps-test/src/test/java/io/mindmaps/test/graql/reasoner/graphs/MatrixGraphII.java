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

package io.mindmaps.test.graql.reasoner.graphs;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;

public class MatrixGraphII extends TestGraph{

    final static String key = "index";
    final static String gqlFile = "matrix-testII.gql";

    public MatrixGraphII(int n, int m){
        super(key, gqlFile);
        buildExtensionalDB(n, m);
        commit();
    }

    public static MindmapsGraph getGraph(int n, int m) {
        return new MatrixGraphII(n, m).graph();
    }

    private void buildExtensionalDB(int n, int m) {
        RoleType Qfrom = mindmaps.getRoleType("Q-from");
        RoleType Qto = mindmaps.getRoleType("Q-to");

        EntityType aEntity = mindmaps.getEntityType("a-entity");
        RelationType Q = mindmaps.getRelationType("Q");
        String[][] aInstancesIds = new String[n+1][m+1];
        Instance aInst = putEntity("a", mindmaps.getEntityType("entity"));
        for(int i = 1 ; i <= n ;i++)
            for(int j = 1 ; j <= m ;j++)
                aInstancesIds[i][j] = putEntity("a" + i + "," + j, aEntity).getId();

        mindmaps.addRelation(Q)
                .putRolePlayer(Qfrom, aInst)
                .putRolePlayer(Qto, mindmaps.getInstance(aInstancesIds[1][1]));

        for(int i = 1 ; i <= n ; i++) {
            for (int j = 1; j <= m; j++) {
                if ( i < n ) {
                    mindmaps.addRelation(Q)
                            .putRolePlayer(Qfrom, mindmaps.getInstance(aInstancesIds[i][j]))
                            .putRolePlayer(Qto, mindmaps.getInstance(aInstancesIds[i+1][j]));
                }
                if ( j < m){
                    mindmaps.addRelation(Q)
                            .putRolePlayer(Qfrom, mindmaps.getInstance(aInstancesIds[i][j]))
                            .putRolePlayer(Qto, mindmaps.getInstance(aInstancesIds[i][j+1]));
                }
            }
        }
    }
}
