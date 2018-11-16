/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.reasoner.graph;

import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import grakn.core.server.Session;
import grakn.core.server.Transaction;

/**
 * Defines a KB based on test 6.10 from Cao p. 82, but arranged in a matrix instead of a tree.
 *
 * @author Kasper Piskorski
 *
 */
@SuppressWarnings("CheckReturnValue")
public class PathMatrixGraph extends ParametrisedTestGraph {

    public PathMatrixGraph(Session session){
        super(session, "pathTest.gql", Label.of("index"));
    }

    @Override
    protected void buildExtensionalDB( int n, int m) {
        Transaction tx = tx();
        EntityType vertex = tx.getEntityType("vertex");
        EntityType startVertex = tx.getEntityType("start-vertex");
        Role arcFrom = tx.getRole("arc-from");
        Role arcTo = tx.getRole("arc-to");

        RelationshipType arc = tx.getRelationshipType("arc");
        putEntityWithResource(tx, "a0", startVertex, key());

        for(int i = 0 ; i < n ;i++) {
            for (int j = 0; j < m; j++) {
                putEntityWithResource(tx, "a" + i + "," + j, vertex, key());
            }
        }

        arc.create()
                .assign(arcFrom, getInstance(tx, "a0"))
                .assign(arcTo, getInstance(tx, "a0,0"));

        for(int i = 0 ; i < n ;i++) {
            for (int j = 0; j < m; j++) {
                if (j < n - 1) {
                    arc.create()
                            .assign(arcFrom, getInstance(tx, "a" + i + "," + j))
                            .assign(arcTo, getInstance(tx, "a" + i + "," + (j + 1)));
                }
                if (i < m - 1) {
                    arc.create()
                            .assign(arcFrom, getInstance(tx, "a" + i + "," + j))
                            .assign(arcTo, getInstance(tx, "a" + (i + 1) + "," + j));
                }
            }
        }
    }

    @Override
    protected void buildExtensionalDB(int n) {
        buildExtensionalDB(n, n);
    }
}
