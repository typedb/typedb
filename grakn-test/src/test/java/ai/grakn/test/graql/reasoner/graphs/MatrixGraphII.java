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
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RoleType;

public class MatrixGraphII extends TestGraph{

    final static String key = "index";
    final static String gqlFile = "matrix-testII.gql";

    public MatrixGraphII(int n, int m){
        super(key, gqlFile);
        buildExtensionalDB(n, m);
        commit();
    }

    public static GraknGraph getGraph(int n, int m) {
        return new MatrixGraphII(n, m).graph();
    }

    private void buildExtensionalDB(int n, int m) {
        RoleType Qfrom = graknGraph.getRoleType("Q-from");
        RoleType Qto = graknGraph.getRoleType("Q-to");

        EntityType aEntity = graknGraph.getEntityType("a-entity");
        RelationType Q = graknGraph.getRelationType("Q");
        String[][] aInstancesIds = new String[n+1][m+1];
        Instance aInst = putEntity("a", graknGraph.getEntityType("entity"));
        for(int i = 1 ; i <= n ;i++)
            for(int j = 1 ; j <= m ;j++)
                aInstancesIds[i][j] = putEntity("a" + i + "," + j, aEntity).getId();

        Q.addRelation()
                .putRolePlayer(Qfrom, aInst)
                .putRolePlayer(Qto, graknGraph.getConcept(aInstancesIds[1][1]));

        for(int i = 1 ; i <= n ; i++) {
            for (int j = 1; j <= m; j++) {
                if ( i < n ) {
                    Q.addRelation()
                            .putRolePlayer(Qfrom, graknGraph.getConcept(aInstancesIds[i][j]))
                            .putRolePlayer(Qto, graknGraph.getConcept(aInstancesIds[i+1][j]));
                }
                if ( j < m){
                    Q.addRelation()
                            .putRolePlayer(Qfrom, graknGraph.getConcept(aInstancesIds[i][j]))
                            .putRolePlayer(Qto, graknGraph.getConcept(aInstancesIds[i][j+1]));
                }
            }
        }
    }
}
