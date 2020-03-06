/*
 * Copyright (C) 2020 Grakn Labs
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

import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;

import static grakn.core.util.GraqlTestUtil.loadFromFile;
import static grakn.core.util.GraqlTestUtil.putEntityWithResource;

@SuppressWarnings("CheckReturnValue")
public class NguyenGraph{

    private final Session session;
    private final static String gqlPath = "test-integration/graql/reasoner/resources/recursion/";
    private final static String gqlFile = "nguyen.gql";
    private final static Label key = Label.of("index");

    public NguyenGraph(Session session){
        this.session = session;
    }

    public final void load(int n) {
        Transaction tx = session.writeTransaction();
        loadFromFile(gqlPath, gqlFile, tx);
        buildExtensionalDB(n, tx);
        tx.commit();
    }

    protected void buildExtensionalDB(int n, Transaction tx) {
        Role Rfrom = tx.getRole("R-rA");
        Role Rto = tx.getRole("R-rB");
        Role qfrom = tx.getRole("Q-rA");
        Role qto = tx.getRole("Q-rB");
        Role Pfrom = tx.getRole("P-rA");
        Role Pto = tx.getRole("P-rB");

        EntityType entity = tx.getEntityType("entity2");
        EntityType aEntity = tx.getEntityType("a-entity");
        EntityType bEntity = tx.getEntityType("b-entity");
        RelationType r = tx.getRelationType("R");
        RelationType p = tx.getRelationType("P");
        RelationType q = tx.getRelationType("Q");

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
}
