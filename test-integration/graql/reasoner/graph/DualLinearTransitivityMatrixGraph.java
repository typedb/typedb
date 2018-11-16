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

import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import grakn.core.server.Session;
import grakn.core.server.Transaction;

public class DualLinearTransitivityMatrixGraph extends ParametrisedTestGraph {

    private final static Label key = Label.of("index");

    public DualLinearTransitivityMatrixGraph(Session session){
        super(session, "dualLinearTransitivity.gql", Label.of("index"));
    }
    
    @Override
    protected void buildExtensionalDB(int n, int m) {
        Transaction tx = tx();
        Role R1from = tx.getRole("R1-from");
        Role R1to = tx.getRole("R1-to");
        Role R2from = tx.getRole("R2-from");
        Role R2to = tx.getRole("R2-to");

        EntityType aEntity = tx.getEntityType("a-entity");
        EntityType bEntity = tx.getEntityType("b-entity");
        RelationshipType R1 = tx.getRelationshipType("R1");
        RelationshipType R2 = tx.getRelationshipType("R2");

        ConceptId[] aInstancesIds = new ConceptId[m+1];
        ConceptId[][] bInstancesIds = new ConceptId[m][n+1];
        aInstancesIds[0] = putEntityWithResource(tx, "a0", tx.getEntityType("start"), key).id();
        aInstancesIds[m] = putEntityWithResource(tx, "a" + m, tx.getEntityType("end"), key).id();
        for(int i = 1 ; i < m ;i++) {
            aInstancesIds[i] = putEntityWithResource(tx, "a" + i, aEntity, key).id();
        }

        for(int i = 1 ; i < m ;i++) {
            for (int j = 1; j <= n; j++) {
                bInstancesIds[i][j] = putEntityWithResource(tx, "b" + i + j, bEntity, key).id();
            }
        }

        for (int i = 0; i < m; i++) {
            R1.create()
                    .assign(R1from, tx.getConcept(aInstancesIds[i]))
                    .assign(R1to, tx.getConcept(aInstancesIds[i + 1]));
        }

        for(int j = 1 ; j <= n ;j++) {
            R2.create()
                    .assign(R2from, tx.getConcept(aInstancesIds[0]))
                    .assign(R2to, tx.getConcept(bInstancesIds[1][j]));
            R2.create()
                    .assign(R2from, tx.getConcept(bInstancesIds[m-1][j]))
                    .assign(R2to, tx.getConcept(aInstancesIds[m]));
            for (int i = 1; i < m - 1; i++) {
                R2.create()
                        .assign(R2from, tx.getConcept(bInstancesIds[i][j]))
                        .assign(R2to, tx.getConcept(bInstancesIds[i + 1][j]));
            }
        }
    }

    @Override
    protected void buildExtensionalDB(int n) {
        buildExtensionalDB(n, n);
    }
}
