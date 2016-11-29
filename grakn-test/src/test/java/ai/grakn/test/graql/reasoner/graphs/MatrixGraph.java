/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
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
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;

public class MatrixGraph extends TestGraph{

    final static String key = "index";
    final static String gqlFile = "matrix-test.gql";

    public MatrixGraph(int n, int m){
        super(key, gqlFile);
        buildExtensionalDB(n, m);
        commit();
    }

    public static GraknGraph getGraph(int n, int m) {
        return new MatrixGraph(n, m).graph();
    }

    private void buildExtensionalDB(int n, int m) {
        RoleType R1from = graknGraph.getRoleType("R1-from");
        RoleType R1to = graknGraph.getRoleType("R1-to");
        RoleType R2from = graknGraph.getRoleType("R2-from");
        RoleType R2to = graknGraph.getRoleType("R2-to");

        EntityType aEntity = graknGraph.getEntityType("a-entity");
        EntityType bEntity = graknGraph.getEntityType("b-entity");
        RelationType R1 = graknGraph.getRelationType("R1");
        RelationType R2 = graknGraph.getRelationType("R2");

        String[] aInstancesIds = new String[m+1];
        String[][] bInstancesIds = new String[m][n+1];
        aInstancesIds[0] = putEntity("a0", graknGraph.getEntityType("start")).getId();
        aInstancesIds[m] = putEntity("a" + m, graknGraph.getEntityType("end")).getId();
        for(int i = 1 ; i < m ;i++)
            aInstancesIds[i] = putEntity("a" + i, aEntity).getId();

        for(int i = 1 ; i < m ;i++)
            for(int j = 1 ; j <= n ;j++)
                bInstancesIds[i][j] = putEntity("b" + i + j, bEntity).getId();

        for (int i = 0; i < m; i++) {
            R1.addRelation()
                    .putRolePlayer(R1from, graknGraph.getConcept(aInstancesIds[i]))
                    .putRolePlayer(R1to, graknGraph.getConcept(aInstancesIds[i + 1]));
        }

        for(int j = 1 ; j <= n ;j++) {
            R2.addRelation()
                    .putRolePlayer(R2from, graknGraph.getConcept(aInstancesIds[0]))
                    .putRolePlayer(R2to, graknGraph.getConcept(bInstancesIds[1][j]));
            R2.addRelation()
                    .putRolePlayer(R2from, graknGraph.getConcept(bInstancesIds[m-1][j]))
                    .putRolePlayer(R2to, graknGraph.getConcept(aInstancesIds[m]));
            for (int i = 1; i < m - 1; i++) {
                R2.addRelation()
                        .putRolePlayer(R2from, graknGraph.getConcept(bInstancesIds[i][j]))
                        .putRolePlayer(R2to, graknGraph.getConcept(bInstancesIds[i+1][j]));
            }
        }
    }
}
