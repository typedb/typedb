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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.kbs;

import ai.grakn.GraknTx;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.test.rule.SampleKBContext;

/**
 * Defines a KB based on test 6.10 from Cao p. 82, but arranged in a matrix instead of a tree.
 *
 * @author Kasper Piskorski
 *
 */
public class PathMatrixKB extends AbstractPathKB {

    private PathMatrixKB(int n, int m){
        super("path-test.gql", Label.of("index"), n, m);
    }

    public static SampleKBContext context(int n, int m) {
        return new PathMatrixKB(n, m).makeContext();
    }

    @Override
    protected void buildExtensionalDB(GraknTx graph, int n, int m) {
        long startTime = System.currentTimeMillis();

        EntityType vertex = graph.getEntityType("vertex");
        EntityType startVertex = graph.getEntityType("start-vertex");
        Role arcFrom = graph.getRole("arc-from");
        Role arcTo = graph.getRole("arc-to");

        RelationshipType arc = graph.getRelationshipType("arc");
        putEntityWithResource(graph, "a0", startVertex, getKey());

        for(int i = 0 ; i < n ;i++) {
            for (int j = 0; j < m; j++) {
                putEntityWithResource(graph, "a" + i + "," + j, vertex, getKey());
            }
        }

        arc.addRelationship()
                .addRolePlayer(arcFrom, getInstance(graph, "a0"))
                .addRolePlayer(arcTo, getInstance(graph, "a0,0"));

        for(int i = 0 ; i < n ;i++) {
            for (int j = 0; j < m; j++) {
                if (j < n - 1) {
                    arc.addRelationship()
                            .addRolePlayer(arcFrom, getInstance(graph, "a" + i + "," + j))
                            .addRolePlayer(arcTo, getInstance(graph, "a" + i + "," + (j + 1)));
                }
                if (i < m - 1) {
                    arc.addRelationship()
                            .addRolePlayer(arcFrom, getInstance(graph, "a" + i + "," + j))
                            .addRolePlayer(arcTo, getInstance(graph, "a" + (i + 1) + "," + j));
                }
            }
        }

        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("PathKBII loading time: " + loadTime + " ms");
    }
}
