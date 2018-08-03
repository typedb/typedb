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

        ConceptId cId = putEntityWithResource(graph, "c", entity, key).id();
        ConceptId dId = putEntityWithResource(graph, "d", entity, key).id();
        ConceptId eId = putEntityWithResource(graph, "e", entity, key).id();

        ConceptId[] aInstancesIds = new ConceptId[n+2];
        ConceptId[] bInstancesIds = new ConceptId[n+2];

        aInstancesIds[n+1] = putEntityWithResource(graph, "a" + (n+1), aEntity, key).id();
        for(int i = 0 ; i <= n ;i++) {
            aInstancesIds[i] = putEntityWithResource(graph, "a" + i, aEntity, key).id();
            bInstancesIds[i] = putEntityWithResource(graph, "b" + i, bEntity, key).id();
        }


        p.create()
                .assign(Pfrom, graph.getConcept(cId))
                .assign(Pto, graph.getConcept(dId));

        r.create()
                .assign(Rfrom, graph.getConcept(dId))
                .assign(Rto, graph.getConcept(eId));

        q.create()
                .assign(qfrom, graph.getConcept(eId))
                .assign(qto, graph.getConcept(aInstancesIds[0]));

        for(int i = 0 ; i <= n ;i++){
            p.create()
                    .assign(Pfrom, graph.getConcept(bInstancesIds[i]))
                    .assign(Pto, graph.getConcept(cId));
            p.create()
                    .assign(Pfrom, graph.getConcept(cId))
                    .assign(Pto, graph.getConcept(bInstancesIds[i]));
            q.create()
                    .assign(qfrom, graph.getConcept(aInstancesIds[i]))
                    .assign(qto, graph.getConcept(bInstancesIds[i]));
            q.create()
                    .assign(qfrom, graph.getConcept(bInstancesIds[i]))
                    .assign(qto, graph.getConcept(aInstancesIds[i+1]));
        }
    }
}
