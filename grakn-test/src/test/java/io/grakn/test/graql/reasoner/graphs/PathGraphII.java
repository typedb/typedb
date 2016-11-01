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

public class PathGraphII extends GenericGraph {

    public static GraknGraph getGraph(int n, int m) {
        final String gqlFile = "path-test.gql";
        getGraph(gqlFile);
        buildExtensionalDB(n, m);
        commit();
        return grakn;
    }

    private static void buildExtensionalDB(int n, int m) {
        long startTime = System.currentTimeMillis();

        EntityType vertex = grakn.getEntityType("vertex");
        EntityType startVertex = grakn.getEntityType("start-vertex");
        RoleType arcFrom = grakn.getRoleType("arc-from");
        RoleType arcTo = grakn.getRoleType("arc-to");

        RelationType arc = grakn.getRelationType("arc");
        grakn.putEntity("a0", startVertex);

        for(int i = 0 ; i < n ;i++)
            for(int j = 0; j < m; j++)
                grakn.putEntity("a" + i +"," + j, vertex);

        grakn.addRelation(arc)
                .putRolePlayer(arcFrom, grakn.getInstance("a0"))
                .putRolePlayer(arcTo, grakn.getInstance("a0,0"));

        for(int i = 0 ; i < n ;i++) {
            for (int j = 0; j < m; j++) {
                if (j < n - 1) {
                    grakn.addRelation(arc)
                            .putRolePlayer(arcFrom, grakn.getInstance("a" + i + "," + j))
                            .putRolePlayer(arcTo, grakn.getInstance("a" + i + "," + (j + 1)));
                }
                if (i < m - 1) {
                    grakn.addRelation(arc)
                            .putRolePlayer(arcFrom, grakn.getInstance("a" + i + "," + j))
                            .putRolePlayer(arcTo, grakn.getInstance("a" + (i + 1) + "," + j));
                }
            }
        }

        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("PathGraphII loading time: " + loadTime + " ms");
    }
}
