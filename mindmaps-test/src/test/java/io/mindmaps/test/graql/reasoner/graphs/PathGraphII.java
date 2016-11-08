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

public class PathGraphII extends TestGraph {

    final static String key = "index";
    final static String gqlFile = "path-test.gql";

    public PathGraphII(int n, int m){
        super(key, gqlFile);
        buildExtensionalDB(n, m);
        commit();
    }

    public static MindmapsGraph getGraph(int n, int m) {
        return new PathGraphII(n, m).graph();
    }

    private void buildExtensionalDB(int n, int m) {
        long startTime = System.currentTimeMillis();

        EntityType vertex = mindmaps.getEntityType("vertex");
        EntityType startVertex = mindmaps.getEntityType("start-vertex");
        RoleType arcFrom = mindmaps.getRoleType("arc-from");
        RoleType arcTo = mindmaps.getRoleType("arc-to");

        RelationType arc = mindmaps.getRelationType("arc");
        putEntity("a0", startVertex);

        for(int i = 0 ; i < n ;i++)
            for(int j = 0; j < m; j++)
                putEntity("a" + i +"," + j, vertex);

        mindmaps.addRelation(arc)
                .putRolePlayer(arcFrom, getInstance("a0"))
                .putRolePlayer(arcTo, getInstance("a0,0"));

        for(int i = 0 ; i < n ;i++) {
            for (int j = 0; j < m; j++) {
                if (j < n - 1) {
                    mindmaps.addRelation(arc)
                            .putRolePlayer(arcFrom, getInstance("a" + i + "," + j))
                            .putRolePlayer(arcTo, getInstance("a" + i + "," + (j + 1)));
                }
                if (i < m - 1) {
                    mindmaps.addRelation(arc)
                            .putRolePlayer(arcFrom, getInstance("a" + i + "," + j))
                            .putRolePlayer(arcTo, getInstance("a" + (i + 1) + "," + j));
                }
            }
        }

        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("PathGraphII loading time: " + loadTime + " ms");
    }
}
