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

package ai.grakn.test.kbs;

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
public abstract class AbstractPathKB extends TestKB {
    private final static Label key = Label.of("index");
    private final String gqlFile;
    private final int n;
    private final int m;

    protected AbstractPathKB(String gqlFile, int n, int m){
        this.gqlFile = gqlFile;
        this.n = n;
        this.m = m;
    }

    protected void buildExtensionalDB(GraknTx tx, int n, int children) {
        long startTime = System.currentTimeMillis();

        EntityType vertex = tx.getEntityType("vertex");
        EntityType startVertex = tx.getEntityType("start-vertex");
        Role arcFrom = tx.getRole("arc-from");
        Role arcTo = tx.getRole("arc-to");

        RelationshipType arc = tx.getRelationshipType("arc");
        putEntity(tx, "a0", startVertex, key);

        int outputThreshold = 500;
        for(int i = 1; i <= n ; i++) {
            int m = IntMath.pow(children, i);
            for (int j = 0; j < m; j++) {
                putEntity(tx, "a" + i + "," + j, vertex, key);
                if (j != 0 && j % outputThreshold ==0) {
                    System.out.println(j + " entities out of " + m + " inserted");
                }
            }
        }

        for (int j = 0; j < children; j++) {
            arc.addRelationship()
                    .addRolePlayer(arcFrom, getInstance(tx, "a0"))
                    .addRolePlayer(arcTo, getInstance(tx, "a1," + j));
        }

        for(int i = 1 ; i < n ;i++) {
            int m = IntMath.pow(children, i);
            for (int j = 0; j < m; j++) {
                for (int c = 0; c < children; c++) {
                    arc.addRelationship()
                            .addRolePlayer(arcFrom, getInstance(tx, "a" + i + "," + j))
                            .addRolePlayer(arcTo, getInstance(tx, "a" + (i + 1) + "," + (j * children + c)));

                }
                if (j!= 0 && j % outputThreshold == 0) {
                    System.out.println("level " + i + "/" + (n - 1) + ": " + j + " entities out of " + m + " connected");
                }
            }
        }

        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("PathKB loading time: " + loadTime + " ms");
    }

    @Override
    public Consumer<GraknTx> build(){
        return (GraknTx tx) -> {
            SampleKBContext.loadFromFile(tx, gqlFile);
            buildExtensionalDB(tx, n, m);
        };
    }
}
