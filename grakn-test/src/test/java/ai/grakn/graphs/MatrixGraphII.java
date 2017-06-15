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
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeLabel;

import java.util.function.Consumer;

public class MatrixGraphII extends TestGraph {

    private final static TypeLabel key = TypeLabel.of("index");
    private final static String gqlFile = "matrix-testII.gql";

    private final int n;
    private final int m;

    public MatrixGraphII(int n, int m){
        this.m = m;
        this.n = n;
    }

    public static Consumer<GraknGraph> get(int n, int m) {
        return new MatrixGraphII(n, m).build();
    }

    @Override
    public Consumer<GraknGraph> build(){
        return (GraknGraph graph) -> {
            loadFromFile(graph, gqlFile);
            buildExtensionalDB(graph, n, m);
        };
    }

    private void buildExtensionalDB(GraknGraph graph, int n, int m) {
        RoleType Qfrom = graph.getRoleType("Q-from");
        RoleType Qto = graph.getRoleType("Q-to");

        EntityType aEntity = graph.getEntityType("a-entity");
        RelationType Q = graph.getRelationType("Q");
        ConceptId[][] aInstancesIds = new ConceptId[n+1][m+1];
        Instance aInst = putEntity(graph, "a", graph.getEntityType("entity2"), key);
        for(int i = 1 ; i <= n ;i++)
            for(int j = 1 ; j <= m ;j++)
                aInstancesIds[i][j] = putEntity(graph, "a" + i + "," + j, aEntity, key).getId();

        Q.addRelation()
                .addRolePlayer(Qfrom, aInst)
                .addRolePlayer(Qto, graph.getConcept(aInstancesIds[1][1]));

        for(int i = 1 ; i <= n ; i++) {
            for (int j = 1; j <= m; j++) {
                if ( i < n ) {
                    Q.addRelation()
                            .addRolePlayer(Qfrom, graph.getConcept(aInstancesIds[i][j]))
                            .addRolePlayer(Qto, graph.getConcept(aInstancesIds[i+1][j]));
                }
                if ( j < m){
                    Q.addRelation()
                            .addRolePlayer(Qfrom, graph.getConcept(aInstancesIds[i][j]))
                            .addRolePlayer(Qto, graph.getConcept(aInstancesIds[i][j+1]));
                }
            }
        }
    }
}
