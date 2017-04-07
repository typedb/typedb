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

package ai.grakn.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeLabel;

import java.util.function.Consumer;

public class NguyenGraph extends TestGraph {

    private final static TypeLabel key = TypeLabel.of("index");
    private final static String gqlFile = "nguyen-test.gql";

    private final int n;

    public NguyenGraph(int n){
        this.n = n;
    }

    public static Consumer<GraknGraph> get(int n) {
        return new NguyenGraph(n).build();
    }

    @Override
    public Consumer<GraknGraph> build(){
        return (GraknGraph graph) -> {
            loadFromFile(graph, gqlFile);
            buildExtensionalDB(graph, n);
        };
    }

    private void buildExtensionalDB(GraknGraph graph, int n) {
        RoleType Rfrom = graph.getRoleType("R-rA");
        RoleType Rto = graph.getRoleType("R-rB");
        RoleType Qfrom = graph.getRoleType("Q-rA");
        RoleType Qto = graph.getRoleType("Q-rB");
        RoleType Pfrom = graph.getRoleType("P-rA");
        RoleType Pto = graph.getRoleType("P-rB");

        EntityType entity = graph.getEntityType("entity2");
        EntityType aEntity = graph.getEntityType("a-entity");
        EntityType bEntity = graph.getEntityType("b-entity");
        RelationType R = graph.getRelationType("R");
        RelationType P = graph.getRelationType("P");
        RelationType Q = graph.getRelationType("Q");

        ConceptId cId = putEntity(graph, "c", entity, key).getId();
        ConceptId dId = putEntity(graph, "d", entity, key).getId();
        ConceptId eId = putEntity(graph, "e", entity, key).getId();

        ConceptId[] aInstancesIds = new ConceptId[n+2];
        ConceptId[] bInstancesIds = new ConceptId[n+2];

        aInstancesIds[n+1] = putEntity(graph, "a" + (n+1), aEntity, key).getId();
        for(int i = 0 ; i <= n ;i++) {
            aInstancesIds[i] = putEntity(graph, "a" + i, aEntity, key).getId();
            bInstancesIds[i] = putEntity(graph, "b" + i, bEntity, key).getId();
        }


        P.addRelation()
                .addRolePlayer(Pfrom, graph.getConcept(cId))
                .addRolePlayer(Pto, graph.getConcept(dId));

        R.addRelation()
                .addRolePlayer(Rfrom, graph.getConcept(dId))
                .addRolePlayer(Rto, graph.getConcept(eId));

        Q.addRelation()
                .addRolePlayer(Qfrom, graph.getConcept(eId))
                .addRolePlayer(Qto, graph.getConcept(aInstancesIds[0]));

        for(int i = 0 ; i <= n ;i++){
            P.addRelation()
                    .addRolePlayer(Pfrom, graph.getConcept(bInstancesIds[i]))
                    .addRolePlayer(Pto, graph.getConcept(cId));
            P.addRelation()
                    .addRolePlayer(Pfrom, graph.getConcept(cId))
                    .addRolePlayer(Pto, graph.getConcept(bInstancesIds[i]));
            Q.addRelation()
                    .addRolePlayer(Qfrom, graph.getConcept(aInstancesIds[i]))
                    .addRolePlayer(Qto, graph.getConcept(bInstancesIds[i]));
            Q.addRelation()
                    .addRolePlayer(Qfrom, graph.getConcept(bInstancesIds[i]))
                    .addRolePlayer(Qto, graph.getConcept(aInstancesIds[i+1]));
        }
    }
}
