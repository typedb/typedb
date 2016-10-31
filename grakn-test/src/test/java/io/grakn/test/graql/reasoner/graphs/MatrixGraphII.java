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

package io.grakn.test.graql.reasoner.graphs;

import io.grakn.MindmapsGraph;
import io.grakn.concept.EntityType;
import io.grakn.concept.RelationType;
import io.grakn.concept.RoleType;

public class MatrixGraphII extends GenericGraph{

    public static MindmapsGraph getGraph(int n, int m) {
        final String gqlFile = "matrix-testII.gql";
        getGraph(gqlFile);
        buildExtensionalDB(n, m);
        commit();
        return mindmaps;
    }

    private static void buildExtensionalDB(int n, int m) {
        RoleType Qfrom = mindmaps.getRoleType("Q-from");
        RoleType Qto = mindmaps.getRoleType("Q-to");

        EntityType aEntity = mindmaps.getEntityType("a-entity");
        RelationType Q = mindmaps.getRelationType("Q");

        for(int i = 1 ; i <= n ;i++)
            for(int j = 1 ; j <= m ;j++)
                mindmaps.putEntity("a" + i + "," + j, aEntity);

        mindmaps.addRelation(Q)
                .putRolePlayer(Qfrom, mindmaps.getInstance("a"))
                .putRolePlayer(Qto, mindmaps.getInstance("a1,1"));

        for(int i = 1 ; i <= n ; i++) {
            for (int j = 1; j <= m; j++) {
                if ( i < n ) {
                    mindmaps.addRelation(Q)
                            .putRolePlayer(Qfrom, mindmaps.getInstance("a" + i + "," + j))
                            .putRolePlayer(Qto, mindmaps.getInstance("a" + (i + 1) + "," + j));
                }
                if ( j < m){
                    mindmaps.addRelation(Q)
                            .putRolePlayer(Qfrom, mindmaps.getInstance("a" + i + "," + j))
                            .putRolePlayer(Qto, mindmaps.getInstance("a" + i + "," + (j + 1)));

                }
            }
        }

    }
}
