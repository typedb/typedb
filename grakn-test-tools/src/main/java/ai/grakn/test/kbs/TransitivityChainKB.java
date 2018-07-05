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
import ai.grakn.concept.Thing;
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
public class TransitivityChainKB extends TestKB {

    private final static Label key = Label.of("index");
    private final static String gqlFile = "quadraticTransitivity.gql";

    private final int n;

    public TransitivityChainKB(int n){
        this.n = n;
    }

    public static SampleKBContext context(int n) {
        return new TransitivityChainKB(n).makeContext();
    }

    @Override
    public Consumer<GraknTx> build(){
        return (GraknTx graph) -> {
            SampleKBLoader.loadFromFile(graph, gqlFile);
            buildExtensionalDB(graph, n);
        };
    }

    private void buildExtensionalDB(GraknTx graph, int n) {
        Role qfrom = graph.getRole("Q-from");
        Role qto = graph.getRole("Q-to");

        EntityType aEntity = graph.getEntityType("a-entity");
        RelationshipType q = graph.getRelationshipType("Q");
        Thing aInst = putEntityWithResource(graph, "a", graph.getEntityType("entity2"), key);
        ConceptId[] aInstanceIds = new ConceptId[n];
        for(int i = 0 ; i < n ;i++) {
            aInstanceIds[i] = putEntityWithResource(graph, "a" + i, aEntity, key).id();
        }

        q.create()
                .assign(qfrom, aInst)
                .assign(qto, graph.getConcept(aInstanceIds[0]));

        for(int i = 0 ; i < n - 1 ; i++) {
                    q.create()
                            .assign(qfrom, graph.getConcept(aInstanceIds[i]))
                            .assign(qto, graph.getConcept(aInstanceIds[i+1]));
        }
    }
}
