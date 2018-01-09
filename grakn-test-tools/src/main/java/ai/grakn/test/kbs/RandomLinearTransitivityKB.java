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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.kbs;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.SampleKBLoader;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Random;
import java.util.function.Consumer;

/**
 *
 * @author Kasper Piskorski
 *
 */
public class RandomLinearTransitivityKB extends TestKB {

    private final static String gqlFile = "linearTransitivity.gql";

    private final int N;

    private RandomLinearTransitivityKB(int N){
        this.N = N;
    }

    public static SampleKBContext context(int N) {
        return new RandomLinearTransitivityKB(N).makeContext();
    }

    @Override
    public Consumer<GraknTx> build(){
        return (GraknTx graph) -> {
            SampleKBLoader.loadFromFile(graph, gqlFile);
            buildExtensionalDB(graph, N);
        };
    }

    private void buildExtensionalDB(GraknTx graph, int N) {
        Role Qfrom = graph.getRole("Q-from");
        Role Qto = graph.getRole("Q-to");

        EntityType aEntity = graph.getEntityType("a-entity");
        RelationshipType Q = graph.getRelationshipType("Q");
        ConceptId[] aInstancesIds = new ConceptId[N];
        for(int i = 0 ; i < N ;i++) {
            aInstancesIds[i] = aEntity.addEntity().getId();
        }

        Random rand = new Random();
        Multimap<Integer, Integer> assignmentMap = HashMultimap.create();
        for(int i = 0 ; i < N ; i++) {
            int from = rand.nextInt(N - 1);
            int to = rand.nextInt(N - 1);
            while(to == from && assignmentMap.get(from).contains(to)) to = rand.nextInt(N - 1);
            Q.addRelationship()
                    .addRolePlayer(Qfrom, graph.getConcept(aInstancesIds[from]))
                    .addRolePlayer(Qto, graph.getConcept(aInstancesIds[to]));
        }
    }
}
