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

package ai.grakn.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeLabel;

import java.util.function.Consumer;

public class PathGraphII extends TestGraph {

    private final static TypeLabel key = TypeLabel.of("index");
    final static String gqlFile = "path-test.gql";

    private final int n;
    private final int m;

    public PathGraphII(int n, int m){
        this.m = m;
        this.n = n;
    }

    public static Consumer<GraknGraph> get(int n, int m) {
        return new PathGraphII(n, m).build();
    }

    @Override
    public Consumer<GraknGraph> build(){
        return (GraknGraph graph) -> {
            loadFromFile(graph, gqlFile);
            buildExtensionalDB(graph, n, m);
        };
    }

    private void buildExtensionalDB(GraknGraph graph, int n, int m) {
        long startTime = System.currentTimeMillis();

        EntityType vertex = graph.getEntityType("vertex");
        EntityType startVertex = graph.getEntityType("start-vertex");
        RoleType arcFrom = graph.getRoleType("arc-from");
        RoleType arcTo = graph.getRoleType("arc-to");

        RelationType arc = graph.getRelationType("arc");
        putEntity(graph, "a0", startVertex, key);

        for(int i = 0 ; i < n ;i++) {
            for (int j = 0; j < m; j++) {
                putEntity(graph, "a" + i + "," + j, vertex, key);
            }
        }

        arc.addRelation()
                .addRolePlayer(arcFrom, getInstance(graph, "a0"))
                .addRolePlayer(arcTo, getInstance(graph, "a0,0"));

        for(int i = 0 ; i < n ;i++) {
            for (int j = 0; j < m; j++) {
                if (j < n - 1) {
                    arc.addRelation()
                            .addRolePlayer(arcFrom, getInstance(graph, "a" + i + "," + j))
                            .addRolePlayer(arcTo, getInstance(graph, "a" + i + "," + (j + 1)));
                }
                if (i < m - 1) {
                    arc.addRelation()
                            .addRolePlayer(arcFrom, getInstance(graph, "a" + i + "," + j))
                            .addRolePlayer(arcTo, getInstance(graph, "a" + (i + 1) + "," + j));
                }
            }
        }

        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("PathGraphII loading time: " + loadTime + " ms");
    }
}
