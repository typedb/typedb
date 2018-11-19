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

import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;

@SuppressWarnings("CheckReturnValue")
public class DiagonalGraph extends ParametrisedTestGraph {

    public DiagonalGraph(Session session) {
        super(session, "diagonalTest.gql", Label.of("name"));
    }

    @Override
    protected void buildExtensionalDB(int n, int m, Transaction tx) {
        Role relFrom = tx.getRole("rel-from");
        Role relTo = tx.getRole("rel-to");

        EntityType entity1 = tx.getEntityType("entity1");
        RelationshipType horizontal = tx.getRelationshipType("horizontal");
        RelationshipType vertical = tx.getRelationshipType("vertical");
        ConceptId[][] instanceIds = new ConceptId[n][m];
        long inserts = 0;
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                instanceIds[i][j] = putEntityWithResource(tx, "a" + i + "," + j, entity1, key()).id();
                inserts++;
                if (inserts % 100 == 0) System.out.println("inst inserts: " + inserts);

            }
        }

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                if (i < n - 1) {
                    vertical.create()
                            .assign(relFrom, tx.getConcept(instanceIds[i][j]))
                            .assign(relTo, tx.getConcept(instanceIds[i + 1][j]));
                    inserts++;
                }
                if (j < m - 1) {
                    horizontal.create()
                            .assign(relFrom, tx.getConcept(instanceIds[i][j]))
                            .assign(relTo, tx.getConcept(instanceIds[i][j + 1]));
                    inserts++;
                }
                if (inserts % 100 == 0) System.out.println("rel inserts: " + inserts);
            }
        }
    }

    @Override
    protected void buildExtensionalDB(int n, Transaction tx) {
        buildExtensionalDB(n, n, tx);
    }
}
