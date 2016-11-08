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
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;

public class NguyenGraph extends TestGraph{

    final static String key = "index";
    final static String gqlFile = "nguyen-test.gql";

    public NguyenGraph(int n){
        super(key, gqlFile);
        buildExtensionalDB(n);
        commit();
    }

    public static MindmapsGraph getGraph(int n) {
        return new NguyenGraph(n).graph();
    }

    private void buildExtensionalDB(int n) {
        RoleType Rfrom = mindmaps.getRoleType("R-rA");
        RoleType Rto = mindmaps.getRoleType("R-rB");
        RoleType Qfrom = mindmaps.getRoleType("Q-rA");
        RoleType Qto = mindmaps.getRoleType("Q-rB");
        RoleType Pfrom = mindmaps.getRoleType("P-rA");
        RoleType Pto = mindmaps.getRoleType("P-rB");

        EntityType entity = mindmaps.getEntityType("entity");
        EntityType aEntity = mindmaps.getEntityType("a-entity");
        EntityType bEntity = mindmaps.getEntityType("b-entity");
        RelationType R = mindmaps.getRelationType("R");
        RelationType P = mindmaps.getRelationType("P");
        RelationType Q = mindmaps.getRelationType("Q");

        String cId = putEntity("c", entity).getId();
        String dId = putEntity("d", entity).getId();
        String eId = putEntity("e", entity).getId();

        String[] aInstancesIds = new String[n+2];
        String[] bInstancesIds = new String[n+2];

        aInstancesIds[n+1] = putEntity("a" + (n+1), aEntity).getId();
        for(int i = 0 ; i <= n ;i++) {
            aInstancesIds[i] = putEntity("a" + i, aEntity).getId();
            bInstancesIds[i] = putEntity("b" + i, bEntity).getId();
        }

        mindmaps.addRelation(R)
                .putRolePlayer(Rfrom, mindmaps.getInstance(dId))
                .putRolePlayer(Rto, mindmaps.getInstance(eId));

        mindmaps.addRelation(P)
                .putRolePlayer(Pfrom, mindmaps.getInstance(cId))
                .putRolePlayer(Pto, mindmaps.getInstance(dId));

        mindmaps.addRelation(Q)
                .putRolePlayer(Qfrom, mindmaps.getInstance(eId))
                .putRolePlayer(Qto, mindmaps.getInstance(aInstancesIds[0]));

        for(int i = 0 ; i <= n ;i++){
            mindmaps.addRelation(P)
                    .putRolePlayer(Pfrom, mindmaps.getInstance(bInstancesIds[i]))
                    .putRolePlayer(Pto, mindmaps.getInstance(cId));
            mindmaps.addRelation(P)
                    .putRolePlayer(Pfrom, mindmaps.getInstance(cId))
                    .putRolePlayer(Pto, mindmaps.getInstance(bInstancesIds[i]));
            mindmaps.addRelation(Q)
                    .putRolePlayer(Qfrom, mindmaps.getInstance(aInstancesIds[i]))
                    .putRolePlayer(Qto, mindmaps.getInstance(bInstancesIds[i]));
            mindmaps.addRelation(Q)
                    .putRolePlayer(Qfrom, mindmaps.getInstance(bInstancesIds[i]))
                    .putRolePlayer(Qto, mindmaps.getInstance(aInstancesIds[i+1]));
        }
    }
}
