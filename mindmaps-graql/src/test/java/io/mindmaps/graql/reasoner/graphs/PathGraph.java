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

import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.core.model.Instance;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.RoleType;
import org.junit.Ignore;

import static com.google.common.math.IntMath.pow;

@Ignore
public class PathGraph extends GenericGraph {

    public static MindmapsTransaction getTransaction(int n, int children) {
        final String gqlFile = "path-test.gql";
        getTransaction(gqlFile);
        buildExtensionalDB(n, children);
        commit();
        return mindmaps;
    }

    private static void buildExtensionalDB(int n, int children) {
        EntityType vertex = mindmaps.getEntityType("vertex");
        RoleType arcFrom = mindmaps.getRoleType("arc-from");
        RoleType arcTo = mindmaps.getRoleType("arc-to");

        RelationType arc = mindmaps.getRelationType("arc");
        putEntity(vertex, "a0");

        for(int i = 1 ; i <= n ;i++) {
            int m = pow(children, i);
            for (int j = 0; j < m; j++) {
                putEntity(vertex, "a" + i + "," + j);
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

    private static Instance putEntity(EntityType type, String name) {
        return mindmaps.putEntity(name.replaceAll(" ", "-").replaceAll("\\.", ""), type).setValue(name);
    }
}
