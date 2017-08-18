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

package ai.grakn.test.graphs;

import ai.grakn.GraknTx;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Label;
import ai.grakn.test.SampleKBContext;
import com.google.common.math.IntMath;

import java.util.function.Consumer;

/**
 *
 * @author Kasper Piskorski
 *
 */
public abstract class AbstractPathGraph extends TestGraph {
    private final static Label key = Label.of("index");
    private final String gqlFile;
    private final int n;
    private final int m;

    protected AbstractPathGraph(String gqlFile, int n, int m){
        this.gqlFile = gqlFile;
        this.n = n;
        this.m = m;
    }

    protected void buildExtensionalDB(GraknTx graph, int n, int children) {
        long startTime = System.currentTimeMillis();

        EntityType vertex = graph.getEntityType("vertex");
        EntityType startVertex = graph.getEntityType("start-vertex");
        Role arcFrom = graph.getRole("arc-from");
        Role arcTo = graph.getRole("arc-to");

        RelationshipType arc = graph.getRelationshipType("arc");
        putEntity(graph, "a0", startVertex, key);

        for(int i = 1 ; i <= n ;i++) {
            int m = IntMath.pow(children, i);
            for (int j = 0; j < m; j++) {
                putEntity(graph, "a" + i + "," + j, vertex, key);
                if (j != 0 && j % 100 ==0) {
                    System.out.println(j + " entities out of " + m + " inserted");
                }
            }
        }

        for (int j = 0; j < children; j++) {
            arc.addRelationship()
                    .addRolePlayer(arcFrom, getInstance(graph, "a0"))
                    .addRolePlayer(arcTo, getInstance(graph, "a1," + j));
        }

        for(int i = 1 ; i < n ;i++) {
            int m = IntMath.pow(children, i);
            for (int j = 0; j < m; j++) {
                for (int c = 0; c < children; c++) {
                    arc.addRelationship()
                            .addRolePlayer(arcFrom, getInstance(graph, "a" + i + "," + j))
                            .addRolePlayer(arcTo, getInstance(graph, "a" + (i + 1) + "," + (j * children + c)));

                }
                if (j!= 0 && j % 100 == 0) {
                    System.out.println("level " + i + "/" + (n - 1) + ": " + j + " entities out of " + m + " connected");
                }
            }
        }

        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("PathGraph loading time: " + loadTime + " ms");
    }

    @Override
    public Consumer<GraknTx> build(){
        return (GraknTx graph) -> {
            SampleKBContext.loadFromFile(graph, gqlFile);
            buildExtensionalDB(graph, n, m);
        };
    }
}
