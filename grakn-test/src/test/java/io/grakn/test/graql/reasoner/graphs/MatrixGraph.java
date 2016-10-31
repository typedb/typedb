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

import io.grakn.GraknGraph;
import io.grakn.concept.EntityType;
import io.grakn.concept.RelationType;
import io.grakn.concept.RoleType;

public class MatrixGraph extends GenericGraph{

    public static GraknGraph getGraph(int n, int m) {
        final String gqlFile = "matrix-test.gql";
        getGraph(gqlFile);
        buildExtensionalDB(n, m);
        commit();
        return grakn;
    }

    private static void buildExtensionalDB(int n, int m) {
        RoleType R1from = grakn.getRoleType("R1-from");
        RoleType R1to = grakn.getRoleType("R1-to");
        RoleType R2from = grakn.getRoleType("R2-from");
        RoleType R2to = grakn.getRoleType("R2-to");

        EntityType aEntity = grakn.getEntityType("a-entity");
        EntityType bEntity = grakn.getEntityType("b-entity");
        RelationType R1 = grakn.getRelationType("R1");
        RelationType R2 = grakn.getRelationType("R2");

        grakn.putEntity("a0", grakn.getEntityType("start"));
        grakn.putEntity("a" + m, grakn.getEntityType("end"));
        for(int i = 1 ; i < m ;i++)
            grakn.putEntity("a" + i, aEntity);

        for(int i = 1 ; i < m ;i++)
            for(int j = 1 ; j <= n ;j++)
                grakn.putEntity("b" + i + j, bEntity);

        for (int i = 0; i < m; i++)
            grakn.addRelation(R1)
                    .putRolePlayer(R1from, grakn.getInstance("a" + i))
                    .putRolePlayer(R1to, grakn.getInstance("a" + (i+1)));

        for(int j = 1 ; j <= n ;j++) {
            grakn.addRelation(R2)
                    .putRolePlayer(R2from, grakn.getInstance("a0"))
                    .putRolePlayer(R2to, grakn.getInstance("b1" + j));
            grakn.addRelation(R2)
                    .putRolePlayer(R2from, grakn.getInstance("b" + (m-1) + j))
                    .putRolePlayer(R2to, grakn.getInstance("a" + m));
            for (int i = 1; i < m - 1; i++) {
                grakn.addRelation(R2)
                        .putRolePlayer(R2from, grakn.getInstance("b" + i + j))
                        .putRolePlayer(R2to, grakn.getInstance("b" + (i+1) + j));
            }
        }
    }
}
