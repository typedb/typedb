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

import static com.google.common.math.IntMath.pow;

public class PathGraph extends TestGraph {

    final static String key = "index";
    final static String gqlFile = "path-test.gql";

    public PathGraph(int n, int children){
        super(key, gqlFile);
        buildExtensionalDB(n, children);
        commit();
    }

    public static GraknGraph getGraph(int n, int children) {
        return new PathGraph(n, children).graph();
    }

    protected void buildExtensionalDB(int n, int children) {
        long startTime = System.currentTimeMillis();

        EntityType vertex = graknGraph.getEntityType("vertex");
        EntityType startVertex = graknGraph.getEntityType("start-vertex");
        RoleType arcFrom = graknGraph.getRoleType("arc-from");
        RoleType arcTo = graknGraph.getRoleType("arc-to");

        RelationType arc = graknGraph.getRelationType("arc");
        putEntity("a0", startVertex);

        for(int i = 1 ; i <= n ;i++) {
            int m = pow(children, i);
            for (int j = 0; j < m; j++) {
                putEntity("a" + i + "," + j, vertex);
                if (j != 0 && j % 100 ==0)
                    System.out.println(j + " entities out of " + m + " inserted");
            }
        }

        for (int j = 0; j < children; j++) {
            arc.addRelation()
                    .putRolePlayer(arcFrom, getInstance("a0"))
                    .putRolePlayer(arcTo, getInstance("a1," + j));
        }

        for(int i = 1 ; i < n ;i++) {
            int m = pow(children, i);
            for (int j = 0; j < m; j++) {
                for (int c = 0; c < children; c++) {
                    arc.addRelation()
                            .putRolePlayer(arcFrom, getInstance("a" + i + "," + j))
                            .putRolePlayer(arcTo, getInstance("a" + (i + 1) + "," + (j * children + c)));

                }
                if (j!= 0 && j % 100 == 0)
                    System.out.println("level " + i + "/" + (n-1) + ": " + j + " entities out of " + m + " connected");
            }
        }

        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("PathGraph loading time: " + loadTime + " ms");
    }
}
