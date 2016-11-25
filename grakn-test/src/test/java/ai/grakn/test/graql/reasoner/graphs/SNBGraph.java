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

package ai.grakn.test.graql.reasoner.graphs;

import ai.grakn.GraknGraph;

public class SNBGraph extends TestGraph{

    public static GraknGraph getGraph() {
        return new SNBGraph().graph();
    }

    @Override
    protected void buildGraph() {
        buildOntology();
        buildInstances();
        buildRelations();
        buildRules();
    }

    @Override
    protected void buildOntology() {
        loadGraqlFile("ldbc-snb-ontology.gql");
        loadGraqlFile("ldbc-snb-product-ontology.gql");
    }

    @Override
    protected void buildRules() { loadGraqlFile("ldbc-snb-rules.gql");}

    @Override
    protected void buildInstances() {
        loadGraqlFile("ldbc-snb-data.gql");
    }
}
