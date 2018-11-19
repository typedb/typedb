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
import grakn.core.graql.concept.Thing;

@SuppressWarnings("CheckReturnValue")
public class TransitivityMatrixGraph extends ParametrisedTestGraph {

    public TransitivityMatrixGraph(Session session) {
        super(session, "quadraticTransitivity.gql",  Label.of("index"));
    }

    @Override
    protected void buildExtensionalDB(int n, int m, Transaction tx){
        Role qfrom = tx.getRole("Q-from");
        Role qto = tx.getRole("Q-to");

        EntityType aEntity = tx.getEntityType("a-entity");
        RelationshipType q = tx.getRelationshipType("Q");
        Thing aInst = putEntityWithResource(tx, "a", tx.getEntityType("entity2"), key());
        ConceptId[][] aInstanceIds = new ConceptId[n][m];
        for(int i = 0 ; i < n ;i++) {
            for (int j = 0; j < m; j++) {
                aInstanceIds[i][j] = putEntityWithResource(tx, "a" + i + "," + j, aEntity, key()).id();
            }
        }

        q.create()
                .assign(qfrom, aInst)
                .assign(qto, tx.getConcept(aInstanceIds[0][0]));

        for(int i = 0 ; i < n ; i++) {
            for (int j = 0; j < m ; j++) {
                if ( i < n - 1 ) {
                    q.create()
                            .assign(qfrom, tx.getConcept(aInstanceIds[i][j]))
                            .assign(qto, tx.getConcept(aInstanceIds[i+1][j]));
                }
                if ( j < m - 1){
                    q.create()
                            .assign(qfrom, tx.getConcept(aInstanceIds[i][j]))
                            .assign(qto, tx.getConcept(aInstanceIds[i][j+1]));
                }
            }
        }
    }

    @Override
    protected void buildExtensionalDB(int n, Transaction tx) {
        buildExtensionalDB(n, n, tx);
    }
}
