/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

public class NguyenGraph extends TestGraph{

    final static String key = "index";
    final static String gqlFile = "nguyen-test.gql";

    public NguyenGraph(int n){
        super(key, gqlFile);
        buildExtensionalDB(n);
        commit();
    }

    public static GraknGraph getGraph(int n) {
        return new NguyenGraph(n).graph();
    }

    private void buildExtensionalDB(int n) {
        RoleType Rfrom = graknGraph.getRoleType("R-rA");
        RoleType Rto = graknGraph.getRoleType("R-rB");
        RoleType Qfrom = graknGraph.getRoleType("Q-rA");
        RoleType Qto = graknGraph.getRoleType("Q-rB");
        RoleType Pfrom = graknGraph.getRoleType("P-rA");
        RoleType Pto = graknGraph.getRoleType("P-rB");

        EntityType entity = graknGraph.getEntityType("entity");
        EntityType aEntity = graknGraph.getEntityType("a-entity");
        EntityType bEntity = graknGraph.getEntityType("b-entity");
        RelationType R = graknGraph.getRelationType("R");
        RelationType P = graknGraph.getRelationType("P");
        RelationType Q = graknGraph.getRelationType("Q");

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

        graknGraph.addRelation(R)
                .putRolePlayer(Rfrom, graknGraph.getInstance(dId))
                .putRolePlayer(Rto, graknGraph.getInstance(eId));

        graknGraph.addRelation(P)
                .putRolePlayer(Pfrom, graknGraph.getInstance(cId))
                .putRolePlayer(Pto, graknGraph.getInstance(dId));

        graknGraph.addRelation(Q)
                .putRolePlayer(Qfrom, graknGraph.getInstance(eId))
                .putRolePlayer(Qto, graknGraph.getInstance(aInstancesIds[0]));

        for(int i = 0 ; i <= n ;i++){
            graknGraph.addRelation(P)
                    .putRolePlayer(Pfrom, graknGraph.getInstance(bInstancesIds[i]))
                    .putRolePlayer(Pto, graknGraph.getInstance(cId));
            graknGraph.addRelation(P)
                    .putRolePlayer(Pfrom, graknGraph.getInstance(cId))
                    .putRolePlayer(Pto, graknGraph.getInstance(bInstancesIds[i]));
            graknGraph.addRelation(Q)
                    .putRolePlayer(Qfrom, graknGraph.getInstance(aInstancesIds[i]))
                    .putRolePlayer(Qto, graknGraph.getInstance(bInstancesIds[i]));
            graknGraph.addRelation(Q)
                    .putRolePlayer(Qfrom, graknGraph.getInstance(bInstancesIds[i]))
                    .putRolePlayer(Qto, graknGraph.getInstance(aInstancesIds[i+1]));
        }
    }
}
