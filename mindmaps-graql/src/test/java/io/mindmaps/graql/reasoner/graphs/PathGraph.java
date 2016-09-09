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

package io.mindmaps.graql.reasoner.graphs;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;

import static com.google.common.math.IntMath.pow;

public class PathGraph extends GenericGraph {

    public static MindmapsGraph getGraph(int n, int children) {
        final String gqlFile = "path-test.gql";
        getGraph(gqlFile);
        buildExtensionalDB(n, children);
        commit();
        return mindmaps;
    }

    private static void buildExtensionalDB(int n, int children) {
        EntityType vertex = mindmaps.getEntityType("vertex");
        EntityType startVertex = mindmaps.getEntityType("start-vertex");
        RoleType arcFrom = mindmaps.getRoleType("arc-from");
        RoleType arcTo = mindmaps.getRoleType("arc-to");

        RelationType arc = mindmaps.getRelationType("arc");
        mindmaps.putEntity("a0", startVertex);

        for(int i = 1 ; i <= n ;i++) {
            int m = pow(children, i);
            for (int j = 0; j < m; j++) {
                mindmaps.putEntity("a" + i + "," + j, vertex);
            }
        }

        for (int j = 0; j < children; j++) {
            mindmaps.addRelation(arc)
                    .putRolePlayer(arcFrom, mindmaps.getInstance("a0"))
                    .putRolePlayer(arcTo, mindmaps.getInstance("a1," + j));
        }

        for(int i = 1 ; i < n ;i++) {
            int m = pow(children, i);
            for (int j = 0; j < m; j++) {
                for (int c = 0; c < children; c++) {
                    mindmaps.addRelation(arc)
                            .putRolePlayer(arcFrom, mindmaps.getInstance("a" + i + "," + j))
                            .putRolePlayer(arcTo, mindmaps.getInstance("a" + (i + 1) + "," + (j * children + c)));

                }
            }
        }
    }
}
