/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.test.kbs;

import ai.grakn.GraknTx;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.test.rule.SampleKBContext;
import com.google.common.math.IntMath;

/**
 * Defines a KB based on test 6.10 from Cao p. 82.
 *
 * @author Kasper Piskorski
 *
 */
public class PathTreeKB extends AbstractPathKB {

    private PathTreeKB(int n, int m){
        super("path-test.gql", Label.of("index"), n, m);
    }

    PathTreeKB(String gqlFile, Label label, int n, int m){
        super(gqlFile, label, n, m);
    }

    public static SampleKBContext context(int n, int children) {
        return new PathTreeKB(n, children).makeContext();
    }

    protected void buildExtensionalDB(GraknTx tx, int n, int children) {
        buildTree(tx, tx.getRole("arc-from"), tx.getRole("arc-to"), n , children);
    }

    void buildTree(GraknTx tx, Role fromRole, Role toRole, int n, int children) {
        long startTime = System.currentTimeMillis();

        EntityType vertex = tx.getEntityType("vertex");
        EntityType startVertex = tx.getEntityType("start-vertex");

        RelationshipType arc = tx.getRelationshipType("arc");
        putEntityWithResource(tx, "a0", startVertex, getKey());

        int outputThreshold = 500;
        for (int i = 1; i <= n; i++) {
            int m = IntMath.pow(children, i);
            for (int j = 0; j < m; j++) {
                putEntityWithResource(tx, "a" + i + "," + j, vertex, getKey());
                if (j != 0 && j % outputThreshold == 0) {
                    System.out.println(j + " entities out of " + m + " inserted");
                }
            }
        }

        for (int j = 0; j < children; j++) {
            arc.addRelationship()
                    .addRolePlayer(fromRole, getInstance(tx, "a0"))
                    .addRolePlayer(toRole, getInstance(tx, "a1," + j));
        }

        for (int i = 1; i < n; i++) {
            int m = IntMath.pow(children, i);
            for (int j = 0; j < m; j++) {
                for (int c = 0; c < children; c++) {
                    arc.addRelationship()
                            .addRolePlayer(fromRole, getInstance(tx, "a" + i + "," + j))
                            .addRolePlayer(toRole, getInstance(tx, "a" + (i + 1) + "," + (j * children + c)));

                }
                if (j != 0 && j % outputThreshold == 0) {
                    System.out.println("level " + i + "/" + (n - 1) + ": " + j + " entities out of " + m + " connected");
                }
            }
        }

        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("PathKB loading time: " + loadTime + " ms");
    }
}
