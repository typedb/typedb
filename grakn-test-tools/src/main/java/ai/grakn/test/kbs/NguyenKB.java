/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.test.kbs;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.SampleKBLoader;

import java.util.function.Consumer;

/**
 *
 * @author Kasper Piskorski
 *
 */
public class NguyenKB extends TestKB {

    private final static Label key = Label.of("index");
    private final static String gqlFile = "nguyen-test.gql";

    private final int n;

    public NguyenKB(int n){
        this.n = n;
    }

    public static SampleKBContext context(int n) {
        return new NguyenKB(n).makeContext();
    }

    @Override
    public Consumer<GraknTx> build(){
        return (GraknTx graph) -> {
            SampleKBLoader.loadFromFile(graph, gqlFile);
            buildExtensionalDB(graph, n);
        };
    }

    private void buildExtensionalDB(GraknTx graph, int n) {
        Role Rfrom = graph.getRole("R-rA");
        Role Rto = graph.getRole("R-rB");
        Role qfrom = graph.getRole("Q-rA");
        Role qto = graph.getRole("Q-rB");
        Role Pfrom = graph.getRole("P-rA");
        Role Pto = graph.getRole("P-rB");

        EntityType entity = graph.getEntityType("entity2");
        EntityType aEntity = graph.getEntityType("a-entity");
        EntityType bEntity = graph.getEntityType("b-entity");
        RelationshipType r = graph.getRelationshipType("R");
        RelationshipType p = graph.getRelationshipType("P");
        RelationshipType q = graph.getRelationshipType("Q");

        ConceptId cId = putEntityWithResource(graph, "c", entity, key).getId();
        ConceptId dId = putEntityWithResource(graph, "d", entity, key).getId();
        ConceptId eId = putEntityWithResource(graph, "e", entity, key).getId();

        ConceptId[] aInstancesIds = new ConceptId[n+2];
        ConceptId[] bInstancesIds = new ConceptId[n+2];

        aInstancesIds[n+1] = putEntityWithResource(graph, "a" + (n+1), aEntity, key).getId();
        for(int i = 0 ; i <= n ;i++) {
            aInstancesIds[i] = putEntityWithResource(graph, "a" + i, aEntity, key).getId();
            bInstancesIds[i] = putEntityWithResource(graph, "b" + i, bEntity, key).getId();
        }


        p.addRelationship()
                .addRolePlayer(Pfrom, graph.getConcept(cId))
                .addRolePlayer(Pto, graph.getConcept(dId));

        r.addRelationship()
                .addRolePlayer(Rfrom, graph.getConcept(dId))
                .addRolePlayer(Rto, graph.getConcept(eId));

        q.addRelationship()
                .addRolePlayer(qfrom, graph.getConcept(eId))
                .addRolePlayer(qto, graph.getConcept(aInstancesIds[0]));

        for(int i = 0 ; i <= n ;i++){
            p.addRelationship()
                    .addRolePlayer(Pfrom, graph.getConcept(bInstancesIds[i]))
                    .addRolePlayer(Pto, graph.getConcept(cId));
            p.addRelationship()
                    .addRolePlayer(Pfrom, graph.getConcept(cId))
                    .addRolePlayer(Pto, graph.getConcept(bInstancesIds[i]));
            q.addRelationship()
                    .addRolePlayer(qfrom, graph.getConcept(aInstancesIds[i]))
                    .addRolePlayer(qto, graph.getConcept(bInstancesIds[i]));
            q.addRelationship()
                    .addRolePlayer(qfrom, graph.getConcept(bInstancesIds[i]))
                    .addRolePlayer(qto, graph.getConcept(aInstancesIds[i+1]));
        }
    }
}
