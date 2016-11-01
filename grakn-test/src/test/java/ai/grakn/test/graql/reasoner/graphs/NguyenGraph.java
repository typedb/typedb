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

public class NguyenGraph extends GenericGraph{

    public static GraknGraph getGraph(int n) {
        final String gqlFile = "nguyen-test.gql";
        getGraph(gqlFile);
        buildExtensionalDB(n);
        commit();
        return grakn;
    }

    private static void buildExtensionalDB(int n) {
        RoleType Rfrom = grakn.getRoleType("R-rA");
        RoleType Rto = grakn.getRoleType("R-rB");
        RoleType Qfrom = grakn.getRoleType("Q-rA");
        RoleType Qto = grakn.getRoleType("Q-rB");
        RoleType Pfrom = grakn.getRoleType("P-rA");
        RoleType Pto = grakn.getRoleType("P-rB");

        EntityType aEntity = grakn.getEntityType("a-entity");
        EntityType bEntity = grakn.getEntityType("b-entity");
        RelationType R = grakn.getRelationType("R");
        RelationType P = grakn.getRelationType("P");
        RelationType Q = grakn.getRelationType("Q");

        grakn.putEntity("a" + (n+1), aEntity);
        for(int i = 0 ; i <= n ;i++) {
            grakn.putEntity("a" + i, aEntity);
            grakn.putEntity("b" + i, bEntity);
        }

        grakn.addRelation(R)
                .putRolePlayer(Rfrom, grakn.getInstance("d"))
                .putRolePlayer(Rto, grakn.getInstance("e"));

        grakn.addRelation(P)
                .putRolePlayer(Pfrom, grakn.getInstance("c"))
                .putRolePlayer(Pto, grakn.getInstance("d"));

        grakn.addRelation(Q)
                .putRolePlayer(Qfrom, grakn.getInstance("e"))
                .putRolePlayer(Qto, grakn.getInstance("a0"));

        for(int i = 0 ; i <= n ;i++){
            grakn.addRelation(P)
                    .putRolePlayer(Pfrom, grakn.getInstance("b" + i))
                    .putRolePlayer(Pto, grakn.getInstance("c"));
            grakn.addRelation(P)
                    .putRolePlayer(Pfrom, grakn.getInstance("c"))
                    .putRolePlayer(Pto, grakn.getInstance("b" + i));
            grakn.addRelation(Q)
                    .putRolePlayer(Qfrom, grakn.getInstance("a" + i))
                    .putRolePlayer(Qto, grakn.getInstance("b" + i));
            grakn.addRelation(Q)
                    .putRolePlayer(Qfrom, grakn.getInstance("b" + i))
                    .putRolePlayer(Qto, grakn.getInstance("a" + (i+1)));
        }

    }
}
