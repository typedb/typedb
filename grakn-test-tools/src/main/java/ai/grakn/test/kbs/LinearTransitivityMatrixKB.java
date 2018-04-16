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

/*-
 * #%L
 * grakn-test-tools
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.RelationshipType;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.SampleKBLoader;

import java.util.function.Consumer;

/**
 *
 * @author Kasper Piskorski
 *
 */
public class LinearTransitivityMatrixKB extends TestKB {

    private final static Label key = Label.of("index");
    private final static String gqlFile = "linearTransitivity.gql";

    private final int n;
    private final int m;

    private LinearTransitivityMatrixKB(int n, int m){
        this.m = m;
        this.n = n;
    }

    public static SampleKBContext context(int n, int m) {
        return new LinearTransitivityMatrixKB(n, m).makeContext();
    }

    @Override
    public Consumer<GraknTx> build(){
        return (GraknTx graph) -> {
            SampleKBLoader.loadFromFile(graph, gqlFile);
            buildExtensionalDB(graph, n, m);
        };
    }

    private void buildExtensionalDB(GraknTx graph, int n, int m) {
        Role Qfrom = graph.getRole("Q-from");
        Role Qto = graph.getRole("Q-to");

        EntityType aEntity = graph.getEntityType("a-entity");
        RelationshipType Q = graph.getRelationshipType("Q");
        ConceptId[][] aInstancesIds = new ConceptId[n+1][m+1];
        Thing aInst = putEntityWithResource(graph, "a", graph.getEntityType("entity2"), key);
        for(int i = 1 ; i <= n ;i++) {
            for (int j = 1; j <= m; j++) {
                aInstancesIds[i][j] = putEntityWithResource(graph, "a" + i + "," + j, aEntity, key).getId();
            }
        }

        Q.addRelationship()
                .addRolePlayer(Qfrom, aInst)
                .addRolePlayer(Qto, graph.getConcept(aInstancesIds[1][1]));

        for(int i = 1 ; i <= n ; i++) {
            for (int j = 1; j <= m; j++) {
                if ( i < n ) {
                    Q.addRelationship()
                            .addRolePlayer(Qfrom, graph.getConcept(aInstancesIds[i][j]))
                            .addRolePlayer(Qto, graph.getConcept(aInstancesIds[i+1][j]));
                }
                if ( j < m){
                    Q.addRelationship()
                            .addRolePlayer(Qfrom, graph.getConcept(aInstancesIds[i][j]))
                            .addRolePlayer(Qto, graph.getConcept(aInstancesIds[i][j+1]));
                }
            }
        }
    }
}
