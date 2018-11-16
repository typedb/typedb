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

@SuppressWarnings("CheckReturnValue")
public class NguyenGraph extends ParametrisedTestGraph {

    public NguyenGraph(Session session) {
        super(session, "recursion/nguyen.gql", Label.of("index"));
    }

    @Override
    protected void buildExtensionalDB(int n) {
        Transaction tx = tx();
        Label key = key();
        Role Rfrom = tx.getRole("R-rA");
        Role Rto = tx.getRole("R-rB");
        Role qfrom = tx.getRole("Q-rA");
        Role qto = tx.getRole("Q-rB");
        Role Pfrom = tx.getRole("P-rA");
        Role Pto = tx.getRole("P-rB");

        EntityType entity = tx.getEntityType("entity2");
        EntityType aEntity = tx.getEntityType("a-entity");
        EntityType bEntity = tx.getEntityType("b-entity");
        RelationshipType r = tx.getRelationshipType("R");
        RelationshipType p = tx.getRelationshipType("P");
        RelationshipType q = tx.getRelationshipType("Q");

        ConceptId cId = putEntityWithResource(tx, "c", entity, key).id();
        ConceptId dId = putEntityWithResource(tx, "d", entity, key).id();
        ConceptId eId = putEntityWithResource(tx, "e", entity, key).id();

        ConceptId[] aInstancesIds = new ConceptId[n+2];
        ConceptId[] bInstancesIds = new ConceptId[n+2];

        aInstancesIds[n+1] = putEntityWithResource(tx, "a" + (n+1), aEntity, key).id();
        for(int i = 0 ; i <= n ;i++) {
            aInstancesIds[i] = putEntityWithResource(tx, "a" + i, aEntity, key).id();
            bInstancesIds[i] = putEntityWithResource(tx, "b" + i, bEntity, key).id();
        }


        p.create()
                .assign(Pfrom, tx.getConcept(cId))
                .assign(Pto, tx.getConcept(dId));

        r.create()
                .assign(Rfrom, tx.getConcept(dId))
                .assign(Rto, tx.getConcept(eId));

        q.create()
                .assign(qfrom, tx.getConcept(eId))
                .assign(qto, tx.getConcept(aInstancesIds[0]));

        for(int i = 0 ; i <= n ;i++){
            p.create()
                    .assign(Pfrom, tx.getConcept(bInstancesIds[i]))
                    .assign(Pto, tx.getConcept(cId));
            p.create()
                    .assign(Pfrom, tx.getConcept(cId))
                    .assign(Pto, tx.getConcept(bInstancesIds[i]));
            q.create()
                    .assign(qfrom, tx.getConcept(aInstancesIds[i]))
                    .assign(qto, tx.getConcept(bInstancesIds[i]));
            q.create()
                    .assign(qfrom, tx.getConcept(bInstancesIds[i]))
                    .assign(qto, tx.getConcept(aInstancesIds[i+1]));
        }
    }

    @Override
    protected void buildExtensionalDB(int n, int children) {
        buildExtensionalDB(n);
    }
}
