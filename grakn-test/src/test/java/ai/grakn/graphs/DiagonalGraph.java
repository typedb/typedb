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

public class DiagonalGraph extends TestGraph {

    private final static TypeLabel key = TypeLabel.of("name");
    private final static String gqlFile = "diagonal-test.gql";

    private final int n;
    private final int m;

    public DiagonalGraph(int n, int m){
        this.m = m;
        this.n = n;
    }

    public static Consumer<GraknGraph> get(int n, int m) {
        return new DiagonalGraph(n, m).build();
    }

    @Override
    public Consumer<GraknGraph> build(){
        return (GraknGraph graph) -> {
            loadFromFile(graph, gqlFile);
            buildExtensionalDB(graph, n, m);
        };
    }

    private void buildExtensionalDB(GraknGraph graph, int n, int m) {
        RoleType relFrom = graph.getRoleType("rel-from");
        RoleType relTo = graph.getRoleType("rel-to");

        EntityType entity1 = graph.getEntityType("entity1");
        RelationType horizontal = graph.getRelationType("horizontal");
        RelationType vertical = graph.getRelationType("vertical");
        ConceptId[][] instanceIds = new ConceptId[n][m];
        long inserts = 0;
        for(int i = 0 ; i < n ;i++)
            for(int j = 0 ; j < m ;j++) {
                instanceIds[i][j] = putEntity(graph, "a" + i + "," + j, entity1, key).getId();
                inserts++;
                if (inserts % 100 == 0) System.out.println("inst inserts: " + inserts);

            }

        for(int i = 0 ; i < n ; i++) {
            for (int j = 0; j < m; j++) {
                if ( i < n - 1 ) {
                    vertical.addRelation()
                            .addRolePlayer(relFrom, graph.getConcept(instanceIds[i][j]))
                            .addRolePlayer(relTo, graph.getConcept(instanceIds[i+1][j]));
                    inserts++;
                }
                if ( j < m - 1){
                    horizontal.addRelation()
                            .addRolePlayer(relFrom, graph.getConcept(instanceIds[i][j]))
                            .addRolePlayer(relTo, graph.getConcept(instanceIds[i][j+1]));
                    inserts++;
                }
                if (inserts % 100 == 0) System.out.println("rel inserts: " + inserts);
            }
        }
        System.out.println("Extensional DB loaded.");
    }
}
