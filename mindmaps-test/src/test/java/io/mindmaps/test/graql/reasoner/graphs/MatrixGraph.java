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
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import java.util.Vector;

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

        String[] aInstancesIds = new String[m+1];
        String[][] bInstancesIds = new String[m][n+1];
        aInstancesIds[0] = putEntity("a0", mindmaps.getEntityType("start")).getId();
        aInstancesIds[m] = putEntity("a" + m, mindmaps.getEntityType("end")).getId();
        for(int i = 1 ; i < m ;i++)
            aInstancesIds[i] = putEntity("a" + i, aEntity).getId();

        for(int i = 1 ; i < m ;i++)
            for(int j = 1 ; j <= n ;j++)
                bInstancesIds[i][j] = putEntity("b" + i + j, bEntity).getId();

        for (int i = 0; i < m; i++) {
            mindmaps.addRelation(R1)
                    .putRolePlayer(R1from, mindmaps.getInstance(aInstancesIds[i]))
                    .putRolePlayer(R1to, mindmaps.getInstance(aInstancesIds[i + 1]));
        }

        for(int j = 1 ; j <= n ;j++) {
            mindmaps.addRelation(R2)
                    .putRolePlayer(R2from, mindmaps.getInstance(aInstancesIds[0]))
                    .putRolePlayer(R2to, mindmaps.getInstance(bInstancesIds[1][j]));
            mindmaps.addRelation(R2)
                    .putRolePlayer(R2from, mindmaps.getInstance(bInstancesIds[m-1][j]))
                    .putRolePlayer(R2to, mindmaps.getInstance(aInstancesIds[m]));
            for (int i = 1; i < m - 1; i++) {
                mindmaps.addRelation(R2)
                        .putRolePlayer(R2from, mindmaps.getInstance(bInstancesIds[i][j]))
                        .putRolePlayer(R2to, mindmaps.getInstance(bInstancesIds[i+1][j]));
            }
        }
    }

    private static Instance putEntity(String id, EntityType type) {
        ResourceType<String> index = mindmaps.getResourceType("index");
        RelationType indexRelation = mindmaps.getRelationType("has-index");
        RoleType indexTarget = mindmaps.getRoleType("has-index-owner");
        RoleType indexValue = mindmaps.getRoleType("has-index-value");
        Instance inst = mindmaps.addEntity(type);
        putResource(inst, index, id, indexRelation, indexTarget, indexValue);
        return inst;
    }

    private static <T> void putResource(Instance instance, ResourceType<T> resourceType, T resource, RelationType relationType,
                                        RoleType targetRole, RoleType valueRole) {
        Resource resourceInstance = mindmaps.putResource(resource, resourceType);
        mindmaps.addRelation(relationType)
                .putRolePlayer(targetRole, instance)
                .putRolePlayer(valueRole, resourceInstance);
    }
}
