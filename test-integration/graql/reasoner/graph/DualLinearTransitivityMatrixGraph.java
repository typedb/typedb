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

public class DualLinearTransitivityMatrixGraph{

    private final Session session;
    private final static String gqlPath = "test-integration/graql/reasoner/resources/";
    private final static String gqlFile = "dualLinearTransitivity.gql";
    private final static Label key = Label.of("index");

    public DualLinearTransitivityMatrixGraph(Session session){
        this.session = session;
    }

    public final void load(int n, int m) {
        Transaction tx = session.writeTransaction();
        loadFromFile(gqlPath, gqlFile, tx);
        buildExtensionalDB(n, m, tx);
        tx.commit();
    }

    protected void buildExtensionalDB(int n, int m, Transaction tx) {
        Role R1from = tx.getRole("R1-from");
        Role R1to = tx.getRole("R1-to");
        Role R2from = tx.getRole("R2-from");
        Role R2to = tx.getRole("R2-to");

        EntityType aEntity = tx.getEntityType("a-entity");
        EntityType bEntity = tx.getEntityType("b-entity");
        RelationType R1 = tx.getRelationType("R1");
        RelationType R2 = tx.getRelationType("R2");

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
}
